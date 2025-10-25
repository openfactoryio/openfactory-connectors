"""
Main entry point for the OPC UA Gateway FastAPI application.

This module initializes the FastAPI app, sets up logging, includes
API routes, and starts the Uvicorn server when run as a script.

Key components:
- `app`: FastAPI instance with attached logger (`app.state.logger`).
- API routes imported from the `api` module.
- Logging configured via `setup_logging()` with level from config.
- Runs Uvicorn server on `OPCUA_GATEWAY_PORT` when executed directly.

To run the gateway:

    python -m opcua.gateway.src.main
"""

import os
import asyncio
import uvloop
import uvicorn
import json
import httpx
import time
from fastapi import FastAPI
from contextlib import asynccontextmanager
from typing import Optional
from openfactory.kafka import KSQLDBClient
from openfactory.schemas.devices import Device
from .registration import register_gateway
from .api import router as api_router
from .utils import setup_logging
from .config import OPCUA_GATEWAY_PORT, LOG_LEVEL, KAFKA_SEND_INTERVAL_MS, KAFKA_QUEUE_MAXSIZE
from .state import _active_device_defs, _active_tasks
from .monitor import monitor_device
from .producer import GlobalAssetProducer
from . import gateway_metrics as gateway_metrics


def log_task_exceptions(task: asyncio.Task, name: Optional[str] = None) -> None:
    """
    Logs exceptions from an asyncio.Task safely and consistently.

    This function inspects an asyncio task upon completion and logs its
    outcome in a standardized way. It handles normal completion, cancellation,
    and exceptional termination, including rare cases where retrieving the
    exception itself fails.

    The function:
      * Logs an info message if the task was cancelled.
      * Logs an error if retrieving the exception raises an unexpected error.
      * Logs the exception and its traceback if the task raised an exception.
      * Logs an info message if the task completed successfully.

    Args:
        task (asyncio.Task): The asyncio task whose result or exception should
            be logged.
        name (Optional[str]): An optional name to identify the task in log
            messages. If not provided, the task's `repr()` will be used.
    """
    if name is None:
        name = repr(task)

    if task.cancelled():
        app.state.logger.info("Task %s cancelled", name)
        return

    try:
        exc = task.exception()  # may raise if something odd happens
    except Exception as e:
        # This should be rare; log it (include traceback).
        app.state.logger.error("Error retrieving exception from task %s: %s", name, e, exc_info=True)
        return

    if exc is not None:
        # exc is the exception instance; provide an exc_info tuple so logging prints the traceback.
        app.state.logger.error("Task %s failed: %s", name, exc, exc_info=(type(exc), exc, exc.__traceback__))
    else:
        app.state.logger.info("Task %s completed cleanly", name)


async def rebuild_gateway_state(app: FastAPI) -> None:
    """
    Rebuild in-memory gateway state from ksqlDB based on OPC UA assignments.

    Only devices assigned to this gateway (from OPCUA_DEVICE_ASSIGNMENT) are loaded.

    Args:
        app (FastAPI): The FastAPI application instance used to access shared state
    """
    logger = app.state.logger
    gateway_id = app.state.gateway_id
    logger.info(f"Rebuilding gateway state for {gateway_id} from ksqlDB...")

    def _fetch_assigned_devices():
        """ Blocking I/O to fetch assigned device UUIDs. """
        query = f"""
        SELECT DEVICE_UUID
        FROM OPCUA_DEVICE_ASSIGNMENT
        WHERE GATEWAY_ID = '{gateway_id}';
        """
        return app.state.ksql.query(query)

    def _fetch_device_configs(device_uuids: list[str]):
        """ Blocking I/O to fetch connector configs for the given devices. """
        if not device_uuids:
            return []
        uuids_str = ",".join(f"'{u}'" for u in device_uuids)
        query = f"""
        SELECT ASSET_UUID, CONNECTOR_CONFIG
        FROM DEVICE_CONNECTOR
        WHERE ASSET_UUID IN ({uuids_str});
        """
        return app.state.ksql.query(query)

    try:
        # Get assigned device UUIDs
        assigned_rows = await asyncio.to_thread(_fetch_assigned_devices)
        assigned_uuids = [r["DEVICE_UUID"] for r in assigned_rows if r.get("DEVICE_UUID")]
        logger.info(f"Found {len(assigned_uuids)} devices assigned to this gateway.")

        if not assigned_uuids:
            logger.info("No devices assigned; nothing to rebuild.")
            return

        # Fetch their connector configs
        config_rows = await asyncio.to_thread(_fetch_device_configs, assigned_uuids)
        logger.debug(f"Fetched {len(config_rows)} connector configs from DEVICE_CONNECTOR.")

        # Populate _active_device_defs
        _active_device_defs.clear()
        for row in config_rows:
            dev_uuid = row.get("ASSET_UUID")
            raw_config = row.get("CONNECTOR_CONFIG")

            if not dev_uuid or not raw_config:
                logger.warning("Skipping malformed ksqlDB row: %s", row)
                continue

            try:
                cfg = json.loads(raw_config)
                device = Device(**cfg)
                _active_device_defs[dev_uuid] = device
                logger.debug(f"Loaded device {dev_uuid} into gateway state.")
                task = asyncio.create_task(monitor_device(device, app))
                _active_tasks[device.uuid] = task
            except Exception as e:
                logger.warning(f"Failed to connect device {dev_uuid}: {e}", exc_info=True)

        logger.info(f"✅ Restored {_active_device_defs.__len__()} devices for gateway {gateway_id}.")

    except Exception as e:
        logger.error(f"❌ Error rebuilding gateway state for {gateway_id}: {e}")


async def _kafka_poll_loop_async(app: FastAPI, interval=0.1):
    """
    Continuously poll the Kafka producer in an asynchronous loop to clear delivery reports
    and prevent the internal queue from filling up.

    If the task is cancelled, it flushes any remaining messages before exiting.

    Args:
        app (FastAPI): The FastAPI application instance used to access shared state.
        interval (float, optional): Number of seconds to sleep between poll calls. Defaults to 0.1.

    Raises:
        asyncio.CancelledError: Raised when the task is cancelled, after which remaining
            messages are flushed and the task exits cleanly.
    """
    try:
        while True:
            app.state.global_producer.poll(0)  # non-blocking
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        app.state.global_producer.flush(5)
        app.state.info("Kafka poll loop task cancelled cleanly.")


async def _kafka_writer_loop_async(app: FastAPI, batch_interval: float = 0.005) -> None:
    """
    Periodically drain the queue and send messages to Kafka in bursts.

    Args:
        app (FastAPI): The FastAPI application instance used to access shared state.
        batch_interval (float): Time interval (in seconds) between flushes. Defaults to 0.005.
    """
    try:
        while True:
            sleep_start = time.perf_counter()
            await asyncio.sleep(batch_interval)
            loop_lag = max(0.0, time.perf_counter() - sleep_start - batch_interval)
            try:
                gateway_metrics.EVENT_LOOP_LAG.labels(gateway=app.state.gateway_id).set(loop_lag)
                gateway_metrics.BATCH_PROCESSED.labels(gateway=app.state.gateway_id).inc()
            except Exception as prom_err:
                app.state.logger.warning(f"Failed to update Prometheus EVENT_LOOP_LAG / BATCH_PROCESSED: {prom_err}")

            batch_start = time.perf_counter()
            batch = []

            # Drain as many messages as are available without blocking
            while not app.state.queue.empty():
                try:
                    batch.append(app.state.queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            if not batch:
                continue

            # Send all messages in one tight loop
            for msg in batch:
                try:
                    app.state.global_producer.send(
                        asset_uuid=msg["asset_uuid"],
                        asset_attribute=msg["asset_attribute"],
                    )
                except Exception as e:
                    app.state.logger.warning(f"Kafka send failed: {e}", exc_info=True)
                finally:
                    app.state.queue.task_done()

            duration = time.perf_counter() - batch_start
            try:
                gateway_metrics.KAFKA_BATCH_PROCESSING_DURATION.labels(gateway=app.state.gateway_id).observe(duration)
                gateway_metrics.BATCH_SIZE.labels(gateway=app.state.gateway_id).set(len(batch))
                gateway_metrics.MSG_SENT.labels(gateway=app.state.gateway_id).inc(len(batch))
            except Exception as prom_err:
                app.state.logger.warning(f"Failed to update Prometheus metrics: {prom_err}")

    except asyncio.CancelledError:
        app.state.logger.info("Kafka writer loop cancelled cleanly.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for the FastAPI application.

    Starts the periodic gateway registration task and cancels it cleanly at shutdown.

    Args:
        app (FastAPI): The FastAPI application instance.

    Yields:
        None
    """
    # Register with coordinator
    app.state.logger.info('Waiting for OPC UA Coordinator registration ...')
    try:
        await register_gateway(app.state.logger, app)
        app.state.logger.info(f'Registered Gateway as {app.state.gateway_id}')
    except Exception as e:
        app.state.logger.error(f"Failed to register gateway at startup: {e}")

    gateway_metrics.BUILD_INFO.info({
        "version": os.environ.get('APPLICATION_VERSION', 'UNKNOWN'),
        "swarm_node": os.environ.get('NODE_HOSTNAME', 'unknown'),
        "gateway": app.state.gateway_id
    })

    # Create global producer
    app.state.logger.info("Starting Kafka Producer")
    app.state.global_producer = GlobalAssetProducer(ksqlClient=app.state.ksql)

    # Start async Kafka poll loop
    app.state.logger.info("Starting Kafka Producer poll task")
    poll_task = asyncio.create_task(_kafka_poll_loop_async(app))
    poll_task.add_done_callback(
        lambda t: log_task_exceptions(t, "_kafka_poll_loop_async")
    )
    app.state._kafka_poll_task = poll_task

    # Start Kafka writer loop
    app.state.logger.info("Starting Kafka Producer writer loop.")
    writer_task = asyncio.create_task(_kafka_writer_loop_async(app, KAFKA_SEND_INTERVAL_MS/1000.0))
    writer_task.add_done_callback(lambda t: log_task_exceptions(t, "_kafka_writer_loop_async"))
    app.state._kafka_writer_task = writer_task

    # Rebuild local in-memory state before starting anything
    await rebuild_gateway_state(app)

    yield  # <-- control returns to FastAPI while the app is running

    # App shutdown logic
    app.state.logger.info("Shutting down OPC UA Gateway ...")

    # Stop poll thread
    if hasattr(app.state, "_kafka_poll_task"):
        app.state._kafka_poll_task.cancel()
        try:
            await app.state._kafka_poll_task
        except asyncio.CancelledError:
            app.state.logger.info("Kafka poll loop task stopped cleanly.")

    # Flush remaining Kafka messages
    if hasattr(app.state, "global_producer") and app.state.global_producer:
        try:
            app.state.logger.info("Flushing pending Kafka messages before shutdown ...")
            app.state.global_producer.flush(5)
            app.state.logger.info("Kafka producer flushed successfully.")
        except Exception as e:
            app.state.logger.warning(f"Error flushing Kafka producer: {e}")

    # Stop Kafka writer loop
    if hasattr(app.state, "_kafka_writer_task"):
        app.state._kafka_writer_task.cancel()
        try:
            await app.state._kafka_writer_task
        except asyncio.CancelledError:
            app.state.logger.info("Kafka writer loop task stopped cleanly.")

app = FastAPI(title="OPCUA Gateway",
              version=os.environ.get('APPLICATION_VERSION'),
              lifespan=lifespan)
app.state.logger = setup_logging(level=LOG_LEVEL)
app.state.gateway_id = 'UNAVAILABLE'
app.state.ksql = KSQLDBClient(os.getenv("KSQLDB_URL"))
app.state.http_client = httpx.AsyncClient(timeout=10.0)
app.state.queue = asyncio.Queue(maxsize=KAFKA_QUEUE_MAXSIZE)

# Expose Prometheus metrics
app.get("/metrics")(gateway_metrics.metrics_endpoint)

# Endpoints router
app.include_router(api_router)


# ----------------------------
# Entry point for local dev
# ----------------------------
if __name__ == "__main__":

    async def main():
        config = uvicorn.Config(
            "opcua.gateway.src.main:app",
            host="0.0.0.0",
            port=OPCUA_GATEWAY_PORT,
            reload=True,
        )
        server = uvicorn.Server(config)
        await server.serve()

    uvloop.run(main())
