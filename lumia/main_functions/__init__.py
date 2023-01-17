import lumia
from types import SimpleNamespace
from lumia.main_functions import objects


prepare = SimpleNamespace(
    emissions = objects.prepare_emis,
    observations = objects.load_observations,
    config = objects.parse_config,
    paths = lumia.paths.setup
)


run = SimpleNamespace(
    forward = objects.forward,
    optimize = objects.optimize,
    validate = objects.validate
)


test = SimpleNamespace(
    adjoint = objects.adjtest,
    model_adjoint = objects.adjtestmod,
    gradient = objects.gradtest,
)
