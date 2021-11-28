# Created by Alvin Wilta 13519163
# Multiversion Concurrency Control Simulation
# Assume rollbacks are cascading and rollbacked transactions are prioritized


import utils as u


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
    used_by    : transaction id that have used this resource version, for cascading rollback
    '''

    def __init__(self, name: str, ver=0, r=0, w=0, used=None):
        self.name = name
        self.ver = ver
        self.r = r
        self.w = w
        self.used_by = set()
        if (used != None):
            self.used_by.add(used)

    def read(self, time: int, ver: int):
        if (int(self.r) >= time):
            print(f'[READ] {self.name}{str(self.ver)}')
        else:
            print(f'[READ] {self.name}{str(self.ver)} changing RTS to {time}')
            self.r = time
        self.used_by.add(ver)

    def write(self, time: int, ver: int):
        print(
            f'[WRITE] {self.name}{str(self.ver)}, overwriting content')
        self.used_by.add(ver)

    def print_content(self):
        print(
            f'name: {self.name} ver: {self.ver} r-ts: {self.r} w-ts: {self.w}')


class Timestamp:
    '''
    class Timestamp for transaction timestamp
    id          : transaction id
    timestamp   : transaction timestamp
    '''

    def __init__(self, id: int, timestamp: None):
        self.id = id
        self.timestamp = id
        if (timestamp != None):
            self.timestamp = timestamp


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
        for res in self.rvl:
            if (res[-1].name == name):
                res[-1].read(time=self.tsl[id], ver=id)
                break

    def write(self, name: str, id: int):
        roll_trans = set()
        for res in self.rvl:
            # iterating to find resource version based on name
            if (res[-1].name == name):
                need_rollback = False
                for i, r in enumerate(reversed(res)):
                    # iterating to find suitable version for writing
                    time = int(self.tsl[id])
                    if (r.r <= time):
                        if (r.w == time):
                            r.write(time=time, ver=id)
                        else:
                            print(
                                f'[CREATE VER] Created new version for {name}{id} with RTS and WTS: {time}')
                            res.append(ResourceVersion(
                                name=name, ver=id, r=time, w=time, used=id))
                        break
                    elif (r.r > time and i == len(res)-1):
                        self.tsl[id] = self.max_tsl + 1
                        self.max_tsl += 1
                        print(
                            f'[ROLLBACK] Changed TS for T{id}={self.max_tsl}')
                        need_rollback = True
                        rollback_tmp = self.rollbacked_transaction(id=id)
                        roll_trans = set.union(
                            roll_trans, rollback_tmp)
                        break
                break
        return (need_rollback, roll_trans)

    def rollbacked_transaction(self, id: int):
        # cari dulu versi yang ada melibatkan T tersebut
        # return semua used_bynya, semua transaksi yang ada di used_by itu akan dirollback
        # delete semua versi terkait
        roll_trans = set()
        for i, ver_list in enumerate(self.rvl):
            for j, ver in enumerate(ver_list):
                if (ver.ver == id):
                    roll_trans = set.union(roll_trans, ver.used_by)
                    self.rvl[i].pop(j)
        print('Transactions to be rollbacked:', roll_trans)
        return roll_trans

    def print_content(self):
        print('==========')
        for res in self.rvl:
            print('#####')
            for r in res:
                r.print_content()
        print('==========')


# Main Function
# a = {id: [Operations1, Operations2], id2: [Operations3, Operations4]}
# trans_list            : separating transaction to their own
# trans_list_ts         : storing timestamps for each transaction [TS-1, TS-2, TS-3, ...]
# trans_list_rollback_n : indicating how many operation will be rollbacked for that transaction
# rollback_index        : indicating where the rollback is applied and which transaction [{index: id}, {index2: id2}, ...]
operations = ['R5x', 'R2y', 'R1y', 'W3y', 'W3z',
              'R5z', 'R2z', 'R1x', 'R4w', 'W3w', 'W5y', 'W5z', 'C1', 'C2', 'C3', 'C4', 'C5']

#op_list, trans_list_id, res_list = u.createTransaction()
op_list, trans_list_id, res_list = u.createTransactionFromCode(
    operations=operations)


u.prettyPrint(op_list)
trans_list = {}
trans_list_ts = []
trans_list_rollback_n = {}
rollback_index = []
for trans in trans_list_id:
    id = input(f'Input TS for transaction {trans}:')
    trans_list_ts.append(id)

# initialize transaction list
for t in trans_list_id:
    trans_list[t] = []
    trans_list_rollback_n[t] = 0
for op in op_list:
    trans_list[op.id].append(op)


s = ManageTS(res_list, trans_list_id, trans_list_ts)
for i, op in enumerate(op_list):
    trans_list_rollback_n[op.id] += 1
    if (op.op == R):
        s.read(id=op.id, name=op.res)
    elif (op.op == W):
        need_rollback, roll_trans = s.write(id=op.id, name=op.res)
        if (need_rollback):
            rollback_index.append({i: op.id})
            rollback_operation_n = trans_list_rollback_n[op.id]
            op_list[i:i] = trans_list[op.id][:rollback_operation_n]
            roll_trans.remove(op.id)
            for id in roll_trans:
                # rollback cascade
                rollback_index.append({i: op.id})
                rollback_operation_n = trans_list_rollback_n[op.id]
                op_list[i:i] = trans_list[op.id][:rollback_operation_n]

for i, rb in enumerate(rollback_index):
    op_list[i:i] = u.Operation(id=rb[i], op=RB, res='')
s.print_content()
u.prettyPrint(op_list)
