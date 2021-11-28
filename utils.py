# Environment untuk simulasi transaksi dbms (input & data structure)

R = 'R'
W = 'W'
C = 'C'
RB = 'RB'


class Operation:
    '''
    # Kelas transaksi, untuk mendefinisikan struct dari transaksi
    id      : id transaksi
    op      : operator (R/W/C)
    res     : resource yang diakses
    '''

    def __init__(self, id: int, op: str, res: str):
        self.id = id
        self.op = op
        self.res = res

    def pr(self):
        return (f'id:{self.id} op:{self.op} res:{self.res}')


def createTransaction():
    '''
    Function for inputting operation data
    '''
    n_commit = 0
    tmp = []
    trans_list = []
    res_list = []
    is_fin = False
    while (not (is_fin)):
        inp = input('specify operations (R/W/C): ')
        opr = inp[0]
        id = int(inp[1])
        if (not(opr == C)):
            res = input('specify resource for operations: ')
        if (id not in trans_list):
            trans_list.append(id)
        if (opr == R or opr == W):
            if (res not in res_list):
                res_list.append(res)
            tmp.append(Operation(id=id, op=opr, res=res))
        elif (opr == C):
            n_commit += 1
            tmp.append(Operation(id=id, op=C, res='commit'))

        else:
            print('invalid operations! ')
        if(len(trans_list) == n_commit):
            is_fin = True

    return tmp, trans_list, res_list


def createTransactionFromCode(operations: list):
    # Can only handle 1-digit transaction id (0-9)
    tmp = []
    res_list = []
    trans_list = []
    for op in operations:
        if (op[0] != C):
            tmp.append(Operation(id=int(op[1]), op=op[0], res=op[2:]))
            if (op[2:] not in res_list):
                res_list.append(op[2:])
            if (int(op[1]) not in trans_list):
                trans_list.append(int(op[1]))
        else:
            tmp.append(Operation(id=int(op[1]), op=op[0], res='commit'))
    return tmp, trans_list, res_list


def prettyPrint(t: list):
    ret = ''
    for x in t:
        if (x.op == C):
            ret += ("C" + str(x.id) + "->")
        elif (x.op == RB):
            ret += ('Rollback T' + x.id)
        else:
            ret += (x.op + str(x.id) + '(' + x.res + ')->')
    print(ret[:-2])
