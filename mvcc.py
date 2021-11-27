class Transaction:
    def __init__(self, op: list, time: int):
        self.op = op
        self.time = time
        self.curr_op = 0

    def at(el: int):
        return self.op[el]


class Op:
    # Enum untuk membedakan r/w/c
    R = 'read'
    W = 'write'
    C = 'commit'


op = [{Op.R: 'A'}, {Op.R: 'B'}, {Op.W: 'B'}]

t1 = Transaction(op, 1)

for x in t1.op:
    print(x)
