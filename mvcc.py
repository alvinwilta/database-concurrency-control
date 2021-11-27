import utils as u

op_list, trans_list, res_list = u.createTransaction()
u.prettyPrint(op_list)

R = 'R'
W = 'W'
C = 'C'


class Timestamp:
    '''
    class timestamp untuk menyimpan tuple versi
    name    : nama resource
    ver     : versi transaksi
    r       : timestamp untuk read
    w       : timestamp untuk write
    '''

    def __init__(self, name: str):
        self.name = name
        self.ver = 0
        self.r = 0
        self.w = 0

    def read(self, time: int):
        self.r = max(self.r, time)
        return True

    def write(self, time: int, ver: int):
        if (self.r > time):
            # Write gagal, harus rollback
            return False
        else:
            if (self.w != time):
                self.ver = ver
                self.r = time
                self.w = time
            return True

    def print_content(self):
        print(
            f'name: {self.name} ver: {self.ver} r-ts: {self.r} w-ts: {self.w}')


class TimestampList:
    '''
    class manajemen timestamp berdasarkan MVCC
    tsl : TimestampList,  list untuk menyimpan list of timestamp dengan berbagai versi yang akan digunakan dalam MVCC
    '''

    def __init__(self, resource: list):
        self.tsl = []
        for res in resource:
            self.tsl.append([Timestamp(name=res)])

    def read(self, time: int, name: str):
        for res in self.tsl:
            if (res[-1].name == name):
                res[-1].read(time=time)
                break

    def write(self, time, name: str, id: int):
        for res in self.tsl:
            if (res.name == name):
                # do something
                break

    def print_content(self):
        print('==========')
        for res in self.tsl:
            print('#####')
            for r in res:
                r.print_content()
        print('==========')


x = TimestampList(res_list)
x.read(4, 'A')
x.print_content()
