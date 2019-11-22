from datetime import datetime, timedelta
from .messaging_tools import colorize

class tinterv:
    def __init__(self, start, end):
        self.start = start
        self.end = end
        self.dt = self.end - self.start

    def __repr__(self):
        return colorize("<c>%s</c> to <c>%s</c>"%(self.start.strftime("%d %b %Y %H:%M"), self.end.strftime("%d %b %Y %H:%M")))

    def __ge__(self, other):
        if isinstance(other, datetime) :
            return self.start >= other
        elif isinstance(other, tinterv):
            return other.within(self)

    def __gt__(self, other):
        if isinstance(other, datetime) :
            return self.start > other
        elif isinstance(other, tinterv):
            return other.within(self)

    def __le__(self, other):
        if isinstance(other, datetime) :
            return self.end <= other
        elif isinstance(other, tinterv):
            return self.within(other)

    def __lt__(self, other):
        if isinstance(other, datetime) :
            return self.end < other
        elif isinstance(other, tinterv):
            return self.within(other)

    def __eq__(self, other):
        return self.start == other.start and self.end == other.end

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.start.__repr__()+self.end.__repr__())

    def overlaps(self, other):
        return self.start < other.end and self.end > other.start

    def within(self, other):
        return self.start >= other.start and self.end <= other.end

    def total_seconds(self):
        return self.dt.total_seconds()

    def describe(self):
        """
        returns a string describing the main characteristics of the region
        """
        line = ''
        line+= '%s to %s'%(
            self.start.strftime('%d %B %Y %H:%M'),
            self.end.strftime('%d %B %Y %H:%M')
        )
        return line

def time_interval(tstr):
    if 'h' in tstr:
        return timedelta(hours=int(tstr.strip('h')))
    else :
        raise NotImplemented