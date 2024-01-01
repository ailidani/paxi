import enum
from typing import Optional, Set, Tuple
from tc_dataclasses import tc_dataclass

class ServerState(enum.Enum):
    Leader = enum.auto()
    Follower = enum.auto()

    def to_tla(self):
        return python_to_tla(self.name)
    


@tc_dataclass
class SlotAndBallots:
    slot: int
    ballot: int


@tc_dataclass
class OpLog:
    ID: str         # id of the server
    active: bool    # is Leader
    slot: int       # slot 
    ballot: int     # int



class SystemState:
    n_servers: int

    """ The TLA+ label """
    action: str

    """ Current slot of the system """
    current_slot: Tuple[int]

    log: Tuple[Tuple[str]]

    state: Tuple[ServerState]

    slotandballot: Tuple[SlotAndBallots]
    
    serverloglocation: str

    __pretty_template__ = """{% for i in range(n_servers) -%}
server {{ i }}: state={{ state[i] }}, slot={{ current_slot[i] }},
{%- if log[i] %}
 log={{ log[i] | oplog }}
{%- else %}
 log=empty
{%- endif -%}
{%- if loop.nextitem -%}
{{ '\n' }}
{%- endif -%}
{%- endfor -%}"""


def python_to_tla(data):
    if hasattr(data, "to_tla"):
        return data.to_tla()
