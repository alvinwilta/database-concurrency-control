# Optimistic Concurrency Control (OCC) simulation w/ Python
# Rayhan Alghifari Fauzta 13519039

from random import choice


# get type of operation in transaction
def getOperation(arr_trans, arr_num):
  arr_op = []
  for i in range(len(arr_num)):
    arr_op.append((arr_num[i], [], []))

  for t in arr_trans:
    num_trans = t[1]
    idx = arr_num.index(num_trans)

    if(t[0] == 'R'):
      if(t[2] not in arr_op[idx][1]):
        arr_op[idx][1].append(t[2])
    elif(t[0] == 'W'):
      if(t[2] not in arr_op[idx][2]):
        arr_op[idx][2].append(t[2])
  
  return arr_op


# get timestamp
def getTimestamp(arr_trans, arr_num):
  arr_timestamp = []

  for n in arr_num:
    arr_timestamp.append((n, getStartTimestamp(arr_trans, n), getCommitTimestamp(arr_trans, n), getCommitTimestamp(arr_trans, n)))

  return arr_timestamp


# get number of transaction
def getNumTransaction(arr_trans):
  arr_num = []

  for t in arr_trans:
    if t[1] not in arr_num:
      arr_num.append(t[1])

  return arr_num


# get timestamp of start
def getStartTimestamp(arr, num):
  start = -1
  i = 0
  stop = False

  while i < len(arr) and not stop:
    if (arr[i][1] == str(num)):
      start = i
      stop = True
    i+=1

  return start


# get timestamp of commit
def getCommitTimestamp(arr, num):
  commit = -1
  i = 0
  finish = False

  while i < len(arr) and not finish:
    if (arr[i][1] == str(num)):
      if (arr[i][0] == 'C'):
        commit = i
        finish = True
    i+=1

  return commit


# check validation
def isValidTransaction(arr, num, arr_timestamp, arr_num, arr_op):
  valid = True
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
  while i < len(check_queue) and valid:
    valid = compareTimestamp(check_queue[i], arr_timestamp[arr_num.index(num)], arr_num, arr_op)
    i+=1
  return valid


# compare timestamp of transaction
def compareTimestamp(TA, TB, arr_num, arr_op):
  if (TB[1] > TA[2]):
    print(f"T{str(TB[0])} begin after T{str(TA[0])}")
    return True
  else:
    if(TB[1] < TA[2] and TB[3] > TA[2]):
      print(f"T{str(TA[0])} finish before T{str(TB[0])}")
      isNotIntersect = True

      for i in range(len(arr_op[arr_num.index(TB[0])][1])):
        if(arr_op[arr_num.index(TB[0])][1][i] in arr_op[arr_num.index(TA[0])][2]):
          isNotIntersect = False
          intersect_item = arr_op[arr_num.index(TB[0])][1][i]

      if(isNotIntersect):
        print(f"T{str(TB[0])} did not read data of T{str(TA[0])}")
      else:
        print(f"T{str(TB[0])} read data of T{str(TA[0])} that is {intersect_item}")

      return isNotIntersect
    
    else:
      print(f"No validation requirements are fulfilled with T{str(TA[0])}")
      return False


# insert rollbacked transaction to current schedule
def insertRollback(arr_trans, rollback_trans):
  change_idx = True
  interval = 2
  idx = len(arr_trans)//3

  for x in reversed(rollback_trans):
    arr_trans.insert(idx, x)
    if(idx - interval > 0 and change_idx):
      idx -= interval
    change_idx = choice([True, False])

  return arr_trans


# print inputted transaction
def printSchedule(arr_trans):
    str = ""

    for type, item, trans in arr_trans:
        if type != "C":
            str = str + type + item + "("+trans+"); "
        else:
            str = str + type + item + "; "

    return str


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
def occ(arr_trans, arr_num, arr_timestamp, arr_op, it):
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
      valid = isValidTransaction(arr_trans, x, arr_timestamp, arr_num, arr_op)

      # write data to db if validation success
      if(valid):
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
      all_trans = tmp_arr + arr_trans
      arr_timestamp = getTimestamp(all_trans, arr_num)
      break
  
  # continue to next transaction
  if(len(arr_trans)!=0):
    occ(arr_trans, arr_num, arr_timestamp, arr_op, it-len(rollback_trans))


# OCC runner
def occRun(arr_trans):
  it = 0
  arr_num = getNumTransaction(arr_trans)
  arr_timestamp = getTimestamp(arr_trans, arr_num)
  arr_op = getOperation(arr_trans, arr_num)
  occ(arr_trans, arr_num, arr_timestamp, arr_op, it)
  return formatPrint(tmp_arr)



# ---- Main Program ---- #
arr_trans = readFile('input_occ.txt')

print("Initial schedule:")
print(printSchedule(arr_trans))
print()
arr_result = occRun(arr_trans)
print("Schedule after OCC:")
print(arr_result)