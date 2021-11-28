# Created by Alvin Wilta 13519163
# Multiversion Concurrency Control Simulation
# Assume rollbacks are cascading and rollbacked transactions are prioritized


import utils as u

op_list, trans_list, res_list = u.createTransaction()
u.prettyPrint(op_list)

R = 'R'
W = 'W'
C = 'C'


class ResourceVersion:
    '''
    class resource version for storing specific version of resource WTS and RTS
    name    : data/resource name
    ver     : resource WTS and RTS version
    r       : RTS (read timestamp)
    w       : WTS (write timestamp)
    TODO: Implement cascade rollback using used
    used    : transaction id that have used this resource version, for cascading rollback
    '''

    def __init__(self, name: str, ver: None, r: None, w: None, used: None):
        self.name = name
        self.ver = 0
        self.r = 0
        self.w = 0
        self.used_by = set()
        if (ver != None):
            self.ver = ver
        if (r != None):
            self.r = r
        if (w != None):
            self.w = w
        if (used != None):
            self.add(used)

    def read(self, time: int, ver: int):
        if (self.r >= time):
            print(f'[READ] {self.name}{str(self.ver)}')
        else:
            print(f'[READ] {self.name}{str(self.ver)} changing RTS to {time}')
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
        self.max_tsl = max(transaction_ts)
        for res in resource:
            self.rvl.append([ResourceVersion(name=res)])
        for i, trans in enumerate(transaction):
            self.tsl[trans] = transaction_ts[i]

    def read(self, id: int, name: str):
        for res in self.rvl:
            if (res[-1].name == name):
                res[-1].read(time=self.tsl[id])
                break

    def write(self, name: str, id: int):
        roll_trans = set()
        for res in self.rvl:
            # iterating to find resource version based on name
            if (res[-1].name == name):
                need_rollback = False
                for i, r in enumerate(reversed(res)):
                    # iterating to find suitable version for writing
                    time = self.tsl[id]
                    if (r.r <= time):
                        if (r.w == time):
                            r.write(time=time, ver=id)
                        else:
                            print(
                                f'[CREATE VER] Created new version for {name}{id} with RTS and WTS: {time}')
                            res.append(ResourceVersion(
                                name=name, ver=id, r=time, w=time, used=id))
                        break
                    elif (r.r > time and i == 0):
                        self.tsl[id] = self.max_tsl + 1
                        self.max_tsl += 1
                        print(
                            f'[ROLLBACK] Changed TS for T{id}={self.max_tsl}')
                        need_rollback = True
                        roll_trans = set.union(
                            roll_trans, rollbacked_transaction(id=id))
                        break
                break
        return (not need_rollback, roll_trans)

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
        return roll_trans

    def print_content(self):
        print('==========')
        for res in self.rvl:
            print('#####')
            for r in res:
                r.print_content()
        print('==========')


# Main Function
trans_list_ts = []
for trans in trans_list:
    trans_list_ts.append(input('Input TS for transaction', trans, ':'))

s = ManageTS(res_list, trans_list, trans_list_ts)
rollback_set = set()
for op in op_list:
    if (op.op == R):
        s.read(id=op.id, name=op.res)
    elif (op.op == W):
        s.write(id=op.id, name=op.res)
