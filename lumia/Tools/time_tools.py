from datetime import datetime, timedelta


class tinterv:
    def __init__(self, start, end):
        self.start = start
        self.end = end
        self.dt = self.end - self.start

    def __repr__(self):
        return "%s to %s"%(self.start.strftime("%d %b %Y %H:%M"), self.end.strftime("%d %b %Y %H:%M"))

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

    def overlap_percent(self, other, dtype='float64'):
        if self.within(other):
            return 1.
        elif self.overlaps(other):
            dt = (min(self.end, other.end)-max(self.start, other.start)).total_seconds()
            return dt/self.total_seconds()
        else :
            return 0.

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