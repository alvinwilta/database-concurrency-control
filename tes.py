a = [1, 2, 3]
b = {2, 3, 4}
for el in b:
    if el not in a:
        a.append(el)
print(a)
