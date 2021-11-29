# Created by Alvin Wilta 13519163
# Multiversion Timestamp Ordering Concurrency Control Simulation
# Assume rollbacks are cascading
# Assume rollbacked transactions are always executed immediately after rollback
# Assume commits are always at the end of each transactions

import utils as u

# Change this wo switch between user input/hardcode
is_hardcoded = True

# Change this to define operations manually, transaction ID must be 1 digit. Ordering is important!
operations = ['R5x', 'R2y', 'R1y', 'W3y', 'W3z',
              'R5z', 'R2z', 'R1x', 'R4w', 'W3w', 'W5y', 'W5z', 'C1', 'C2', 'C3', 'C4', 'C5']

R = 'R'
W = 'W'
C = 'C'
RB = 'RB'


class ResourceVersion:
    '''
    class resource version for storing specific version of resource WTS and RTS
    name    : data/resource name
    ver     : resource WTS and RTS version
    r       : RTS (read timestamp)
    w       : WTS (write timestamp)
    used_by : transaction id that have used this resource version, for cascading rollback
    '''

    def __init__(self, name: str, ver=0, r=0, w=0, used=None):
        self.name = name
        self.ver = ver
        self.r = r
        self.w = w
        self.used_by = set()
        if (used != None):
            self.used_by.add(int(used))

    def read(self, time: int, ver: int):
        if (int(self.r) >= time):
            print(f'[T{str(ver)} READ {self.name}] {self.name}{str(self.ver)}')
        else:
            print(
                f'[T{str(ver)} READ {self.name}] {self.name}{str(self.ver)}, RTS={time} (changed)')
            self.r = time
        self.used_by.add(int(ver))

    def write(self, time: int, ver: int):
        print(
            f'[T{str(ver)} WRITE {self.name}] {self.name}{str(self.ver)}, overwriting content')
        self.used_by.add(int(ver))

    def print_content(self):
        print(
            f'{self.name}{self.ver}: r-ts: {self.r}; w-ts: {self.w}')


class ManageTS:
    '''
    class timestamp management for MVCC (singleton class)
    rvl : ResourceVersionList,  list of list of resource versions for MVCC - [[A1, A2],[B0,B3], ...]
    tsl : TimeStampList, Dictionary of timestamp - {id: timestamp, id2: timestamp2, ...}
    '''

    def __init__(self, resource: list, transaction: list, transaction_ts: list):
        self.rvl = []
        self.tsl = {}
        self.max_tsl = int(max(transaction_ts))
        for res in resource:
            self.rvl.append([ResourceVersion(name=res)])
        for i, trans in enumerate(transaction):
            self.tsl[int(trans)] = int(transaction_ts[i])

    def read(self, id: int, name: str):
        found = False
        for res in self.rvl:
            if (res[-1].name == name):
                for i, r in enumerate(reversed(res)):
                    if (r.r <= int(self.tsl[id]) or i == len(res)-1):
                        res[len(res)-1-i].read(time=self.tsl[id], ver=id)
                        found = True
                        break
                if (found):
                    break

    def write(self, name: str, id: int):
        roll_trans = []
        for res in self.rvl:
            # iterating to find resource version based on name
            if (res[-1].name == name):
                need_rollback = False
                for i, r in enumerate(reversed(res)):
                    # iterating to find suitable version for writing
                    time = int(self.tsl[id])
                    if (r.r < time):
                        if (r.w == time):
                            r.write(time=time, ver=id)
                        else:
                            print(
                                f'[T{id} WRITE {name}] Created {name}{time} with RTS and WTS: {time}')
                            res.append(ResourceVersion(
                                name=name, ver=id, r=time, w=time, used=id))
                        break
                    elif (r.r >= time and i == len(res)-1):
                        self.tsl[id] = self.max_tsl + 1
                        self.max_tsl += 1
                        print(
                            f'[ROLLBACK] Changed TS for T{id}={self.max_tsl}')
                        need_rollback = True
                        roll_trans = self.rollbacked_transaction(id=id)
                        break
                break
        return (need_rollback, roll_trans)

    def rollbacked_transaction(self, id: int):
        # Get all transaction that is using resource version created by this transaction
        roll_trans = []
        for i, ver_list in enumerate(self.rvl):
            for j, ver in enumerate(ver_list):
                if (ver.ver == id):
                    tmp = ver.used_by
                    for el in tmp:
                        if int(el) not in roll_trans:
                            roll_trans.append(int(el))
                    self.rvl[i].pop(j)
        print('Transactions to be rollbacked:', end=' ')
        for roll in roll_trans:
            print(f'T{str(roll)}', end=' ')
        print()
        return roll_trans

    def print_content(self):
        print('==========')
        for res in self.rvl:
            print(f'[RESOURCE {res[0].name}]')
            for r in res:
                r.print_content()
        print('==========')


def enterTimestamp():
    trans_list_ts = []
    print('Input TS for each transaction, press enter to assume TS(Ti) = i')
    for trans in trans_list_id:
        id = input(f'TS for T{trans}: ')
        if (id == ''):
            trans_list_ts.append(trans)
        else:
            trans_list_ts.append(int(id))
    return trans_list_ts


# Main Function
# trans_list            : separating transaction to itself - {id: [Operation1, Operation2], id2: [Operation3, Operation4], ...}
# trans_list_ts         : storing timestamps for each transaction - [TS-1, TS-2, TS-3, ...]
# trans_list_rollback_n : indicating how many operation will be rollbacked for that transaction - {id:n, id2:n2, ...}
# rollback_index        : indicating where the rollback is applied and which transaction - [{index: id}, {index2: id2}, ...]

# managing inputs
if (is_hardcoded):
    op_list, trans_list_id, res_list = u.createTransactionFromCode(
        operations=operations)
else:
    op_list, trans_list_id, res_list = u.createTransaction()

# initialize required data
u.prettyPrint(op_list)
trans_list = {}
rollback_index = []
trans_list_rollback_n = {}
trans_list_ts = enterTimestamp()

# initialize transaction list
for t in trans_list_id:
    trans_list[t] = []
    trans_list_rollback_n[t] = 0
for op in op_list:
    trans_list[op.id].append(op)

# executing MVCC protocol
s = ManageTS(res_list, trans_list_id, trans_list_ts)
print('[STARTED]')
for i, op in enumerate(op_list):
    trans_list_rollback_n[op.id] += 1
    if (op.op == R):
        s.read(id=op.id, name=op.res)
    elif (op.op == W):
        need_rollback, roll_trans = s.write(id=op.id, name=op.res)
        if (need_rollback):
            op_remain = len(op_list)-1-i
            rollback_index.append([i, op.id])
            rollback_operation_n = trans_list_rollback_n[op.id]
            op_list[i:i] = trans_list[op.id][:rollback_operation_n-1]
            roll_trans.pop(0)
            for ids in roll_trans:
                # rollback cascade
                rollback_index.append([i, ids])
                rollback_operation_n = trans_list_rollback_n[ids]
                op_list[-op_remain:-
                        op_remain] = trans_list[ids][:rollback_operation_n]
for rb in reversed(rollback_index):
    op_list[rb[0]:rb[0]] = [u.Operation(id=rb[1], op=RB, res='commit')]
print('[FINISHED]')

# printing states
s.print_content()
u.prettyPrint(op_list)
