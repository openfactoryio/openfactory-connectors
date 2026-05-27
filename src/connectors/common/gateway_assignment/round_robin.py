class RoundRobinGatewayAssignmentMixin:
    """
    Simple round-robin gateway assignment strategy.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # internal round-robin index
        self._gateway_rr_index = 0

    def assign_gateway(self) -> str:
        """
        Assign a gateway using a round-robin strategy.

        Returns:
            str: Selected gateway UUID.

        Raises:
            RuntimeError: If no gateways are available.
        """

        if not self.gateways:
            raise RuntimeError("No gateways available")

        gateway_uuid = self.gateways[self._gateway_rr_index]

        self._gateway_rr_index = (
            self._gateway_rr_index + 1
        ) % len(self.gateways)

        return gateway_uuid
