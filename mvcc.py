# Environment untuk simulasi transaksi dbms (input & data structure)
class Transaction:
    '''
    # Kelas transaksi, untuk mendefinisikan struct dari transaksi
    op      : list of dict dari operasi yang dilakukan
    time    : timestamp dari transaksi
    curr_op : pointer untuk mengakses operasi dan untuk rollback
    '''

    def __init__(self, id: int, op: str, res: str):
        self.id = id
        self.op = op
        self.res = res

    def pr():
        print('test')
        return (f'id:{self.id} op:{self.op} res:{self.res}')


R = 'R'
W = 'W'
C = 'C'


def createOp(op: str, res: str):
    return {'op': op, 'res': res}


def createTransaction():
    n_commit = 0
    tmp = []
    trans_list = []
    is_fin = False
    while (not (is_fin)):
        inp = input('specify operations (R/W/C) ex. R1: ')
        opr = inp[0]
        id = int(inp[1])
        if (not(opr == C)):
            res = input('specify resource for operations: ')
        if (id not in trans_list):
            trans_list.append(id)
        if (opr == R or opr == W):
            tmp.append(Transaction(id=id, op=opr, res=res))
        elif (opr == C):
            n_commit += 1
            tmp.append(Transaction(id=id, op=C, res='commit'))

        else:
            print('invalid operations! ')
        if(len(trans_list) == n_commit):
            is_fin = True

    return tmp, trans_list


def prettyPrint(t: list):
    for x in t:
        if (x.op == C):
            print("C" + str(x.id) + "->", end="")
        else:
            print(f'{x.op}{str(x.id)}({x.res})->', end="")


# Asumsi operasi yang dilakukan t1 akan terurut dari indeks 0 hingga akhir
x, y = createTransaction()

for el in x:
    print(f'id:{el.id} op:{el.op} res:{el.res}')
print(y)

prettyPrint(x)
