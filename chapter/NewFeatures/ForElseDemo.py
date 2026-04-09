class ForElseDemo:
    def __init__(self):
        pass


    def main(self):
        pass


    def forElseDemoMethod(self, breakAt:int):
        for i in range(5):

            print(i)

            if i == breakAt:
                break

        else:
            print(f'Else block hs fully executed on items within the range :: {i}')

'''
Python will execute the else block only if the for loop iterates all items in the iterables without hitting a break statement.
If Python encounters a break statement, it’ll skip the else block entirely.
If the iterables has no items, Python executes the else block immediately.
'''

if __name__ == '__main__':
    ForElseDemo().forElseDemoMethod(2)

    ForElseDemo().forElseDemoMethod(200)
