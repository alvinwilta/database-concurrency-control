# Optimistic Concurrency Control (OCC) simulation w/ Python
# Rayhan Alghifari Fauzta 13519039

import random

# check validation
def isValidTransaction(arr, num, arr_timestamp, arr_num, arr_operation):
  isCurrentValid = True
  check_queue = []
  for x in arr_timestamp:
    if(int(x[3]) < arr_timestamp[arr_num.index(num)][3]):
      check_queue.append(x)
  
  if (check_queue):
    print(f"T{num} checking with T : ", end='')
    for x in check_queue:
      print(x[0], end=' ')
    print()
  else:
    print(f"T{num} did not have to be checked")


  # check from current transaction to all transaction before
  i = 0
  while i < len(check_queue) and isCurrentValid:
    isCurrentValid = compareTS(check_queue[i], arr_timestamp[arr_num.index(num)], arr_num, arr_operation)
    i+=1
  return isCurrentValid


# compare timestamp of transaction
def compareTS(TI, TJ, arr_num, arr_operation):
  if (TI[2] < TJ[1]):
    print(f"T{str(TJ[0])} begin after T{str(TI[0])}")
    return True
  else:
    if(TJ[1] < TI[2] and TI[2] < TJ[3]):
      print(f"T{str(TI[0])} finish before T{str(TJ[0])}")
      isNotIntersect = True

      for i in range(len(arr_operation[arr_num.index(TJ[0])][1])):
        if(arr_operation[arr_num.index(TJ[0])][1][i] in arr_operation[arr_num.index(TI[0])][2]):
          isNotIntersect = False
          intersect_item = arr_operation[arr_num.index(TJ[0])][1][i]

      if(isNotIntersect):
        print(f"T{str(TJ[0])} did not read data of T{str(TI[0])}")
      else:
        print(f"T{str(TJ[0])} read data of T{str(TI[0])} that is {intersect_item}")

      return isNotIntersect
    
    else:
      print(f"No validation requirements are fulfilled with T{str(TI[0])}")
      return False


# insert rollbacked transaction to current schedule
def insertRollback(arr_trans, rollback_trans):
  change_idx = True
  interval = 2
  idx = len(arr_trans)//3

  for x in reversed(rollback_trans):
    arr_trans.insert(idx, x)
    if(idx - interval > 0 and change_idx):
      idx = idx - interval
    change_idx = random.choice([True, False])

  return arr_trans


# print format
def formatPrint(arr):
  format = ""

  for x in arr:
    activity = x[0] + x[1]
    if(x[0] != 'C'):
      activity += "(" +x[2] + "); " 
    else:
      activity += "; "
    format += activity

  return format


# get type of operation in transaction
def getTransactionOperation(arr, arr_num):
  arr_operation = []
  for i in range(len(arr_num)):
    arr_operation.append((arr_num[i], [], []))

  for x in arr:
    num_transaction = x[1]
    idx = arr_num.index(num_transaction)

    if(x[0] == 'R'):
      if(x[2] not in arr_operation[idx][1]):
        arr_operation[idx][1].append(x[2])
    elif(x[0] == 'W'):
      if(x[2] not in arr_operation[idx][2]):
        arr_operation[idx][2].append(x[2])
  
  return arr_operation


# get timestamp
def getArrTS(arr_num, arr_trans):
  arr_timestamp = []

  for x in arr_num:
    arr_timestamp.append((x, getStartTS(arr_trans, x), getCommitTS(arr_trans, x), getCommitTS(arr_trans, x)))

  return arr_timestamp


# get number of transaction
def getNumTransaction(arr):
  arr_num = []

  for x in arr:
    if x[1] not in arr_num:
      arr_num.append(x[1])

  return arr_num


# get timestamp of start
def getStartTS(arr, num):
  start = -1
  i = 0
  stop = False

  while i < len(arr) and not stop:
    if (arr[i][1] == str(num)):
      start = i
      stop = True
    i+=1

  return start


# get commit of transaction
def getCommitTS(arr, num):
  finish = -1
  i = 0
  stop = False

  while i < len(arr) and not stop:
    if (arr[i][1] == str(num)):
      if (arr[i][0] == 'C'):
        finish = i
        stop = True
    i+=1

  return finish


# print inputted transaction
def printTrans(arr_trans):
    str = ""

    for type, item, trans in arr_trans:
        if type != "C":
            str = str + type + item + "("+trans+"); "
        else:
            str = str + type + item + "; "

    return str


# read from file
def readFile(filename):
    arr_trans = []

    f = open(filename, "r")
    t = f.read()
    t = t[:-1]
    arr_t = t.split("; ")

    arr_trans = []
    for t in arr_t:
        t = str(t)
        type = t[0]
        if type != "C":
            item = t[-2]
            tr = t[1:-3]
        else:
            item = ""
            tr = t[1:]

        arr_trans.append((type, tr, item))
    f.close()

    return arr_trans

tmp_arr = []
tmp_dict = {}
# OCC
def occ(arr_trans, arr_num, arr_timestamp, arr_operation, it):
  stop = False

  # iterate through each transaction
  while(len(arr_trans)!=0):
    index = arr_num.index(arr_trans[0][1])

    if(arr_timestamp[index][1] == it):
      print(f"T{arr_timestamp[index][0]} start")

    it+=1
    if(arr_trans[0][0] == 'R'):
      print(f"T{arr_trans[0][1]} read {arr_trans[0][2]}")
      tmp_arr.append(arr_trans.pop(0))

    elif(arr_trans[0][0] == 'W'):
      print(f"T{arr_trans[0][1]} write {arr_trans[0][2]}")

      if(arr_trans[0][1] not in tmp_dict):
        tmp_dict[arr_trans[0][1]] = []

      tmp_dict[arr_trans[0][1]].append(arr_trans[0][2])
      tmp_arr.append(arr_trans.pop(0))

    # validate before commit
    elif(arr_trans[0][0] == 'C'):
      print()
      x = arr_trans[0][1]
      print(f'Validating T{x}')
      is_valid = isValidTransaction(arr_trans, x, arr_timestamp, arr_num, arr_operation)

      # write data to db if validation success
      if(is_valid):
        print(f"T{x} validated")

        if(x in tmp_dict):
          print(f"T{x} write {tmp_dict[x]} to database")
        print(f"T{x} commit\n")
      
      # rollback if fail
      else:
        print(f"T{x} failed to validate")
        print("Commence rollback...\n")

        if(x in tmp_dict): 
          tmp_dict.pop(x)
        stop = True

      tmp_arr.append(arr_trans.pop(0))

    if(stop):
      rollback_trans = []
      i = 0

      while i < len(tmp_arr):
        if (tmp_arr[i][1] == x):
          rollback_trans.append(tmp_arr[i])
          tmp_arr.remove(tmp_arr[i])
          i -=1
        i+=1

      arr_trans = insertRollback(arr_trans, rollback_trans)
      all_arr = tmp_arr + arr_trans
      arr_timestamp = getArrTS(arr_num, all_arr)
      break
  
  # continue to next transaction
  if(len(arr_trans)!=0):
    occ(arr_trans, arr_num, arr_timestamp, arr_operation, it-len(rollback_trans))


# OCC runner
def run_occ(arr_trans):
  it = 0
  arr_num = getNumTransaction(arr_trans)
  arr_timestamp = getArrTS(arr_num, arr_trans)
  arr_operation = getTransactionOperation(arr_trans, arr_num)
  occ(arr_trans, arr_num, arr_timestamp, arr_operation, it)
  return formatPrint(tmp_arr)



# ---- Main Program ---- #
arr_trans = readFile('input_occ.txt')

print("Initial schedule:")
print(printTrans(arr_trans))
print()
arr_result = run_occ(arr_trans)
print("Schedule after OCC:")
print(arr_result)