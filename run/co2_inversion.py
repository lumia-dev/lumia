import lumia
from lumia.models import footprints
from lumia.optimizer.scipy_optimizer import Optimizer

dconf = lumia.read_config('inversion.yaml', machine='laptop')
obs = footprints.Observations.from_tar(dconf.observations.file.path)
emis = footprints.Data.from_dconf(dconf, start=dconf.run.start, end=dconf.run.end)
model = footprints.Transport(**dconf.model)
mapping = footprints.Mapping.init(dconf, emis)
prior = lumia.PriorConstraints.setup(dconf.run.paths, mapping)

optim = Optimizer(prior=prior, model=model, mapping=mapping, observations=obs, settings=dconf.congrad)
x_opt = optim.solve()