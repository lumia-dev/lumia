#!/usr/bin/env python

class Categories:
    def __init__(self, rcf=None):
        self.list = []
        if rcf is not None :
            self.setup(rcf)

    def add(self, name):
        if not name in self.list :
            setattr(self, name, Category(name))
            self.list.append(name)
        else :
            raise RuntimeError("Category %s already exists!"%name)

    def __getitem__(self, item):
        if item in self.list :
            return getattr(self, item)
        else :
            raise KeyError("Category %s not initialized yet!"%item)

    def __iter__(self):
        for item in self.list :
            yield getattr(self, item)

    def setup(self, rcf):
        catlist = rcf.get('emissions.categories')
        for cat in catlist :
            self.add(cat)
            self[cat].optimize = rcf.get('emissions.%s.optimize'%cat, totype=bool)
            self[cat].is_ocean = rcf.get('emissions.%s.is_ocean'%cat, totype=bool, default=False)
            if self[cat].optimize :
                self[cat].uncertainty = rcf.get('emissions.%s.error'%cat)
                self[cat].uncertainty_type = rcf.get('emissions.%s.error_type'%cat, default='tot')
                self[cat].min_uncertainty = rcf.get('emissions.%s.error_min'%cat, default=0)
                self[cat].horizontal_correlation = rcf.get('emissions.%s.corr'%cat)
                self[cat].temporal_correlation = rcf.get('emissions.%s.tcorr'%cat)
                self[cat].optimization_interval = rcf.get('optimization.interval')

    def __len__(self):
        return len(self.list)


class Category:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return self.name

    def __eq__(self, other):
        return self.name == other