import lumia
from lumia.optimizer.scipy_optimizer import Optimizer

dconf = lumia.read_config('inversion.yaml', machine='laptop')
obs = lumia.Observations.from_tar(dconf.observations.file.path)
emis = lumia.models.footprints.Data.from_dconf(dconf, start=dconf.run.start, end=dconf.run.end)
model = lumia.models.footprints.Transport(**dconf.model)
mapping = lumia.models.footprints.Mapping.init(dconf, emis)
prior = lumia.prior.PriorConstraints.setup(dconf.run.paths, mapping)

optim = Optimizer(prior=prior, model=model, mapping=mapping, observations=obs, settings=dconf.congrad)
x_opt = optim.solve()