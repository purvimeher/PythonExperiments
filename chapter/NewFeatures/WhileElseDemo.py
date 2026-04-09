class WhileElseDemo:
    def __init__(self):
        pass

    def WhileElseMethod(self, breakAt:int):
        index: int = 0
        while index <= 100:
            print(index)
            if index == breakAt:
                break
            index += 1
        else:
            print(f'This is from while else block :: {index}')


if __name__ == '__main__':
    breakAt: int = 10
    WhileElseDemo().WhileElseMethod(breakAt)
    breakAt: int = 150
    WhileElseDemo().WhileElseMethod(breakAt)
