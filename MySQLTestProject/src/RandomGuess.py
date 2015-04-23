'''
Created on Apr 16, 2015

@author: robert
'''
from random import randint

# Generates a number from 1 through 10 inclusive
random_number = randint(1, 10)
guesses_left = 3

# Start your game!
while guesses_left > 0 :
    print "You have ", guesses_left, "Guesses Left"
    guesses_left -=1
    guess = int(raw_input("Guess a number from 1-10"))
    if guess == random_number:
        print "You Win!"
        break
else:
    print "Sorry, You Lose! The correct number is: ",random_number
