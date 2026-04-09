class TryDemo:
    def __init__(self):
        pass


    def main(self):
        print('main')


    def myMethod(self, age):
        print('myMethod', age)
        try:
            if age > 0:
                print(f'Valid age: {age} is entered')
        except Exception as e:
            print(f'Invalid age: {age} is entered')
        else:
            print(f'Else part is executed only if try is successful')
        finally:
            print(f'Finally is executed!!')



if __name__ == '__main__':
    TryDemo().main()
    TryDemo().myMethod(1)
    TryDemo().myMethod(-1)
    TryDemo().myMethod('A')

