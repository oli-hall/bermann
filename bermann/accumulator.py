class Accumulator(object):

    def __init__(self, value):
        self.value = value or 0

    def add(self, term):
        self.value += term
