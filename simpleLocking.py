# Created by Akifa Nabil Ufairah 13519179
# Simple Locking Concurrency Control and Two Phase Locking Simulation
# Assume commits are always at the end of each transactions
# Using Wound-wait algorithm for deadlock prevention

import utils as u

class Lock:
    def __init__(self, lockType, lockedResource):
        self.lockType = lockType
        self.lockedResource = lockedResource

class Transaction:
    def __init__(self, transactionID, transactionState, timeStamp):
        self.transactionID = transactionID
        self.transactionState = transactionState
        self.lockedResources = []
        self.timeStamp = timeStamp
    
    def releaseLockedResource(self):
        releasedResources = []
        for lock in self.lockedResources:
            print("UL("+str(lock.lockedResource)+")")
            releasedResources.append(lock.lockedResource)
        self.lockedResources = []
        return releasedResources

class SimpleLocking:
    def __init__(self, process, listTransId, resources):
        self.process = process
        self.transactions = {}
        ts = 1
        for id in listTransId:
            self.transactions[id] = Transaction(id,'Active',ts)
            ts += 1
        self.resources = resources 
        # self.operations = operations
        # self.transactions = transactions
        self.lockList = []
        self.iterator = 0
    
    def runOp(self, operation, transactionID, resource):
        # Hapus dari Process
        self.deleteCurrentOperation()

        # Tampilkan Output
        print(operation+str(transactionID)+"("+str(resource+")"))

    def writeLock(self, transactionID, resource, op):
        # Cari lock teradap resource pada lockList
        alreadylocked = False
        lockedByOthers = False

        for lock in (self.transactions[transactionID].lockedResources):
            if (lock.lockedResource == resource):
                alreadylocked = True
        
        if (alreadylocked):
            self.runOp(op,transactionID, resource)
        else:
            lockedByOthers = None
            for t in self.transactions.values():
                if (t.transactionID != transactionID):
                    for lock in (t.lockedResources):
                        if (lock.lockedResource == resource):
                            lockedByOthers = t.transactionID
                            break
            
            if (lockedByOthers != None):
                self.woundWait(transactionID, lockedByOthers, op, resource)
            else:
                # Grant XL
                print("XL"+str(transactionID)+"("+str(resource)+")")
                self.transactions[transactionID].lockedResources.append(Lock('XL',resource))
                self.runOp(op,transactionID, resource)

    def commit(self, transactionID):
        releasedResources = self.transactions[transactionID].releaseLockedResource()
        self.deleteCurrentOperation()
        print("C" + str(transactionID))

        for i in range (len(self.process)):
            if (self.transactions[self.process[i].id].transactionState == 'Waiting' and self.process[i].res in releasedResources):
                self.transactions[self.process[i].id].transactionState = 'Active'
                self.iterator = i
                break                


    def abort(self, transactionID):
        self.transactions[transactionID].releaseLockedResource()
        self.transactions[transactionID].transactionState = 'Aborted'
        print("Aborting transaction with ID", transactionID)


    def deleteCurrentOperation(self):
        del self.process[self.iterator]
    
    def woundWait(self, requestingTransactionId, holdingTransactionId, operation, resource):
        if (self.transactions[requestingTransactionId].timeStamp < self.transactions[holdingTransactionId].timeStamp):
            # abort holding transaction
            self.abort(holdingTransactionId)
            self.writeLock(requestingTransactionId, resource, operation)
        else:
            # waiting for holding transaction
            print("Transaction with ID", requestingTransactionId, "is waiting ...")
            self.transactions[requestingTransactionId].transactionState = 'Waiting'
            self.incIterator()
    
    def incIterator(self):
        if (self.iterator == len(self.process) - 1):
            self.iterator = 0
            for t in self.transactions.values():
                if (t.transactionState == 'Waiting'):
                    t.transactionState = 'Active'
        else:
                self.iterator += 1


    def run(self):
        print()
        print("Concurrency control with Wound-Wait Deadlock Prevention Strategy")
        while (len(self.process) != 0):
            if (self.transactions[self.process[self.iterator].id].transactionState != 'Waiting' and self.transactions[self.process[self.iterator].id].transactionState != 'Aborted'):
                if (self.process[self.iterator].op == 'C'):
                    self.commit(self.process[self.iterator].id)
                else:
                    self.writeLock(self.process[self.iterator].id,self.process[self.iterator].res,self.process[self.iterator].op)
            else:
                if (self.transactions[self.process[self.iterator].id].transactionState == 'Aborted'):
                    self.deleteCurrentOperation()
                else:
                    self.incIterator()


class TwoPhaseLocking:
    def __init__(self, process, listTransId, resources):
        self.process = process
        self.transactions = {}
        ts = 1
        for id in listTransId:
            self.transactions[id] = Transaction(id,'Active',ts)
            ts += 1
        self.resources = resources 
        # self.operations = operations
        # self.transactions = transactions
        self.lockList = []
        self.iterator = 0
    
    def runOp(self, operation, transactionID, resource):
        # Hapus dari Process
        self.deleteCurrentOperation()

        # Tampilkan Output
        print(operation+str(transactionID)+"("+str(resource+")"))

    def writeLock(self, transactionID, resource, op):
        # Cari lock teradap resource pada lockList
        alreadylocked = False
        lockedByOthers = False

        for lock in (self.transactions[transactionID].lockedResources):
            if (lock.lockedResource == resource):
                alreadylocked = True
        
        if (alreadylocked):
            self.runOp(op,transactionID, resource)
        else:
            lockedByOthers = None
            for t in self.transactions.values():
                if (t.transactionID != transactionID):
                    for lock in (t.lockedResources):
                        if (lock.lockedResource == resource):
                            lockedByOthers = t.transactionID
                            break
            
            if (lockedByOthers != None):
                self.woundWait(transactionID, lockedByOthers, 'XL', op, resource)
            else:
                # Grant XL
                print("XL"+str(transactionID)+"("+str(resource)+")")
                self.transactions[transactionID].lockedResources.append(Lock('XL',resource))
                self.runOp(op,transactionID, resource)


    def readLock(self, transactionID, resource, op):
        # Cari lock teradap resource pada lockList
        alreadylocked = False

        for lock in (self.transactions[transactionID].lockedResources):
            if (lock.lockedResource == resource):
                alreadylocked = True
                break

        # Handle lock
        if (alreadylocked):
            self.runOp(op,transactionID,resource)
        else:
            xlByOthers = None
            for t in self.transactions.values():
                if (t.transactionID != transactionID):
                    for lock in (t.lockedResources):
                        if (lock.lockedResource == resource and lock.lockType == 'XL'):
                            xlByOthers = t.transactionID
                            break
                            
            if (xlByOthers != None):
                self.woundWait(transactionID, xlByOthers, 'SL', op, resource)
            else:
                # Grant SL
                print("SL"+str(transactionID)+"("+str(resource)+")")
                self.transactions[transactionID].lockedResources.append(Lock('SL',resource))
                self.runOp(op,transactionID,resource)


    def commit(self, transactionID):
        releasedResources = self.transactions[transactionID].releaseLockedResource()
        self.deleteCurrentOperation()
        print("C" + str(transactionID))

        for i in range (len(self.process)):
            if (self.transactions[self.process[i].id].transactionState == 'Waiting' and self.process[i].res in releasedResources):
                self.transactions[self.process[i].id].transactionState = 'Active'
                self.iterator = i
                break                


    def abort(self, transactionID):
        self.transactions[transactionID].releaseLockedResource()
        self.transactions[transactionID].transactionState = 'Aborted'
        print("Aborting transaction with ID", transactionID)


    def deleteCurrentOperation(self):
        del self.process[self.iterator]
    
    def woundWait(self, requestingTransactionId, holdingTransactionId, lockType, operation, resource):
        if (self.transactions[requestingTransactionId].timeStamp < self.transactions[holdingTransactionId].timeStamp):
            # abort holding transaction
            self.abort(holdingTransactionId)
            if (lockType == 'SL'):
                self.readLock(requestingTransactionId, resource, operation)
            else: # 'XL'
                self.writeLock(requestingTransactionId, resource, operation)
        else:
            # waiting for holding transaction
            print("Transaction with ID", requestingTransactionId, "is waiting ...")
            self.transactions[requestingTransactionId].transactionState = 'Waiting'
            self.incIterator()
    
    def incIterator(self):
        if (self.iterator == len(self.process) - 1):
            self.iterator = 0
            for t in self.transactions.values():
                if (t.transactionState == 'Waiting'):
                    t.transactionState = 'Active'
        else:
                self.iterator += 1


    def run(self):
        lockTypeForTransaction = {}
        for operation in self.process:
            if (operation.id not in lockTypeForTransaction):
                lockTypeForTransaction[operation.id] = {}
            
            if operation.op == 'W':
                lockTypeForTransaction[operation.id][operation.res] = 'XL'
            elif (operation.op == 'R' and operation.res not in lockTypeForTransaction[operation.id]):
                lockTypeForTransaction[operation.id][operation.res] = 'SL'

        while (len(self.process) != 0):
            if (self.transactions[self.process[self.iterator].id].transactionState != 'Waiting' and self.transactions[self.process[self.iterator].id].transactionState != 'Aborted'):
                if (self.process[self.iterator].op == 'C'):
                    self.commit(self.process[self.iterator].id)
                elif (lockTypeForTransaction[self.process[self.iterator].id][self.process[self.iterator].res] == 'SL'):
                    self.readLock(self.process[self.iterator].id,self.process[self.iterator].res,self.process[self.iterator].op)
                elif (lockTypeForTransaction[self.process[self.iterator].id][self.process[self.iterator].res] == 'XL'):
                    self.writeLock(self.process[self.iterator].id,self.process[self.iterator].res,self.process[self.iterator].op)
                else:
                    print("Invalid Operations '"+str(self.process[self.iterator].op)+"'")
            else:
                if (self.transactions[self.process[self.iterator].id].transactionState == 'Aborted'):
                    self.deleteCurrentOperation()
                else:
                    self.incIterator()



process, listTransId, resources = u.createTransaction()
u.prettyPrint(process)

print("""
Pilihan Jenis Locking Protokol : 
1. Simple Locking (exclusive locks only)
2. Two Phase Locking (using exclusive & shared locks)
""")

protokol = int(input("Masukkan nomor pilihan (1/2): "))

while (protokol != 1 and protokol != 2):
    print("Pilihan protokol tidak valid. Masukkan input 1/2")
    protokol = int(input("Masukkan nomor pilihan : "))

if (protokol == 1):
    concurrencyProcessing = SimpleLocking(process, listTransId, resources)
    concurrencyProcessing.run()
else:
    concurrencyProcessing = TwoPhaseLocking(process, listTransId, resources)
    concurrencyProcessing.run()