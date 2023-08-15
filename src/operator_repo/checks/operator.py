from collections.abc import Iterator

from .. import Operator
from . import CheckResult, Fail


def check_upgrade(operator: Operator) -> Iterator[CheckResult]:
    """Validate upgrade graphs for all channels"""
    all_channels = operator.channels | {operator.default_channel} - {None}
    for channel in sorted(all_channels):
        try:
            channel_bundles = operator.channel_bundles(channel)
            channel_head = operator.head(channel)
            graph = operator.update_graph(channel)
            dangling_bundles = {
                x for x in channel_bundles if x not in graph and x != channel_head
            }
            if dangling_bundles:
                yield Fail(
                    f"Channel {channel} has dangling bundles: {dangling_bundles}"
                )
        except Exception as exc:
            yield Fail(str(exc))
