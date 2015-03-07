import random

def GenerateRandomDelay(x):
    if x == 0:
        return 0
    return random.randint(1,x)