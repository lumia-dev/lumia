#!/usr/bin/env python

from lumia.obsdb import obsdb
from matplotlib.pyplot import figure, tight_layout
from numpy import inf
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.widgets import Slider
from matplotlib.pyplot import show


class obsdb(obsdb):
    def plotobs_mpl(self, y1, y2=[], options={}, figsize=(20, 6), saveto=None):

        if isinstance(y1, str):
            y1 = [y1]

        site = self.sites.iloc[0]
        dbs = self.observations.loc[self.observations.site == site.code]

        fig = figure(figsize=figsize)
        ax = fig.add_subplot()
        ax.set_title(site.get('name'))
        ax.grid()
        ax.set_xlim(self.observations.time.min(), self.observations.time.max())

        lines_y1 = []
        for var in y1 :
            lines_kw = options.get(var, {})
            lines_kw['label'] = lines_kw.get('label', var)
            lines_y1.append(ax.plot(dbs.time, dbs.loc[:, var], **lines_kw)[0])

        lines_y2 = []
        if len(y2) > 0 :
            tx = ax.twinx()
            for var in y2 :
                lines_kw = options.get(var, {})
                lines_kw['label'] = lines_kw.get('label', var)
                lines_y2.append(tx.plot(dbs.time, dbs.loc[:, var], **lines_kw)[0])

        # Add legend
        lns = lines_y1+lines_y2
        labs = [l.get_label() for l in lns]
        ax.legend(lns, labs)

        tight_layout()

        def update_fig(i):
            site = self.sites.iloc[i]
            dbs = self.observations.loc[self.observations.site == site.code]
            ax.set_title(site.get('name'))
            #ax.set_xlim(dbs.time.min(), dbs.time.max())
            ymin, ymax = inf, -inf
            for ivar, var in enumerate(y1) :
                lines_y1[ivar].set_data(dbs.time, dbs.loc[:, var])
                ymin = min(dbs.loc[:, var].min(), ymin)
                ymax = max(dbs.loc[:, var].max(), ymax)
            try :
                ax.set_ylim(ymin, ymax)
            except :
                print(site.code)

            if len(y2) > 0 :
                ymin, ymax = inf, -inf
                for ivar, var in enumerate(y2) :
                    lines_y2[ivar].set_data(dbs.time, dbs.loc[:, var])
                    ymin = min(dbs.loc[:, var].min(), ymin)
                    ymax = max(dbs.loc[:, var].max(), ymax)
                tx.set_ylim(ymin, ymax)
            fig.canvas.draw_idle()

        if saveto is not None :
            with PdfPages(saveto) as pdf :
                for isite in range(self.sites.shape[0]):
                    update_fig(isite)
                    pdf.savefig()

        # Add slider:
        slax = fig.add_axes([0.25, 0.03, 0.5, 0.02])
        sl = Slider(slax, 'Site', 0, self.sites.shape[0]-1, valinit=0, valfmt='%i', valstep=1)

        sl.on_changed(update_fig)

        show()
