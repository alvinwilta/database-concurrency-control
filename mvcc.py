# Dibuat oleh Alvin Wilta 13519163
class Transaction:
    '''
    # Kelas transaksi, untuk mendefinisikan struct dari transaksi
    op      : list of dict dari operasi yang dilakukan
    time    : timestamp dari transaksi
    curr_op : pointer untuk mengakses operasi dan untuk rollback
    '''

    def __init__(self, op: list, time: int):
        self.op = op
        self.time = time
        self.curr_op = 0


class Op:
    # Enum untuk membedakan r/w/c
    R = 'read'
    W = 'write'
    C = 'commit'


# Asumsi operasi yang dilakukan t1 akan terurut dari indeks 0 hingga akhir
op = [{Op.R: 'A'}, {Op.R: 'B'}, {Op.W: 'B'}]

t1 = Transaction(op, 1)

for x in t1.op:
    print(x)
