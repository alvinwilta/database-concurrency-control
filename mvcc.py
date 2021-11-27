class Transaction:
    def __init__(self, name: str, op: list, time: int):
        self.name = name
        self.op = op
        self.time = time

    def at(el: int):
        return self.op[el]


class Op:
    # Enum untuk membedakan r/w/c
    R = 'read'
    W = 'write'
    C = 'commit'
