# Observation

<mark>This module needs revision, the doc below is for now a placeholder</mark>

## Observation module(s)

The observations are stored in a `lumia.obsdb.obsdb` object (**lumia/obsdb/__init__.py**). Several derived classes exist, or can be constructed by the user to extend its basic features.

### Object structure:

The `obsdb` class has two main data attributes:

- *observations* is a **DataFrame** object (from the **pandas** library), and contains a list of observations and their characteristics (time, mixing ratio, uncertainties, background concentration, etc.)
- *sites* is another **DataFrame**, and contains information common to groups of observations (e.g. coordinates)