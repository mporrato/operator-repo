from typing import Iterator, Tuple

from .. import Operator


def check_upgrade(operator: Operator) -> Iterator[Tuple[str, str]]:
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
                yield "fail", f"Channel {channel} has dangling bundles: {dangling_bundles}."
        except Exception as e:
            yield "fail", str(e)
