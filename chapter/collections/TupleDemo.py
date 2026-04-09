
# A tuple is a list that cannot change. Python refers to a value that cannot change as immutable. So by definition, a tuple is an immutable list.
numbers = (1,2,3)
print(numbers)

print(type(numbers))

# unpacking tuple into individual variables
x,y,z = numbers

print(x,y,z)

x=1000

print(x,y,z)

# partial unpack
colors = ('red', 'green', 'blue', 'yellow')
print(colors)
print(colors)

red, blue, *other = colors
print(red, blue, *other)

# merging both tuples
allelements = (*numbers, *colors)
print(allelements)


# varible arguments as a parameters in function

range = (1,2,3,4,5,6,7,8,9,10)

x, y, *other = range
print(x, y)

def addupOnlyVariable(x, y, *args):
    total = x + y
    for arg in args:
        arg += arg

    print(f'Total X and Y is  :: {total}')
    print(f'Total of variable arguments :: {arg}')

addupOnlyVariable(x, y, *other)