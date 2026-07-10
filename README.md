# scalelink

[![Deploy to PyPI](https://github.com/ONSdigital/scalelink/actions/workflows/deploy_pypi.yaml/badge.svg?branch=main)](https://github.com/ONSdigital/scalelink/actions/workflows/deploy_pypi.yaml)
[![PyPI version](https://badge.fury.io/py/scalelink.svg)](https://pypi.org/project/scalelink/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/scalelink.svg)](#)
[![Code style: ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

An implementation of the probabilistic data linkage method Scalelink, described in [Goldstein *et al*. (2017)][scalelink].

This implementation has been created in response to recommendation six of the [Joined Up Data in Government: The Future of Data Linking Methods][guidance] cross-government review.

This work is authored by the Data Linkage team within the Methodology and Quality Directorate (MQD) of the Office for National Statistics (ONS). This team researches novel data linkage methods and applications to facilitate production of high-quality linked datasets for national statistics.

## Pre-requisites

- Python 3.10 or 3.11
- PySpark >=3.0.0, <4.0.0

## Method

The Scalelink method is a novel probabilistic data linkage method. It uses a scaling algorithm based on correspondence analysis. A key potential advantage is that it utilises linkage variable dependence. In contrast, the current gold-standard probabilistic data linkage method, the Fellegi-Sunter algorithm, assumes that linkage variables are independent, an assumption which is violated to at least some extent in all linkage problems. For example, forenames are correlated with middle names, surnames, age, gender, home address, etc.

The Scalelink method is **experimental**. This implementation has been open-sourced to facilitate further research regarding the suitability of the Scalelink method for real-world data linkage, particularly on Big Data. The authors **do not** currently endorse using the Scalelink method to produce linked datasets for research, analysis or statistics.

## Installation and use

Please see the [User Guide][user-guide].

## Contact

For questions, support or feedback about the `scalelink` package, please email [DataLinkage@ons.gov.uk](mailto:DataLinkage@ons.gov.uk).

## Acknowledgements

The authors would like to acknowledge the following collaborators for their contributions to the Scalelink research project within the Office for National Statistics:
 - William Browne (University of Bristol)
 - Christopher Charlton (University of Bristol)
 - James Doidge (Intensive Care National Audit & Research Centre)
 - Harvey Goldstein (University of Bristol and University College London)
 - Katie Harron (University College London)
 - Leah Maizey (Office for National Statistics)
 - Josie Plachta (Office for National Statistics)
 - Rachel Shipsey (Office for National Statistics)
 - Paul Smith (University of Southampton)

The authors would also like to acknowledge the following individuals for their support in making this code public:
 - Dominic Bean (Office for National Statistics)
 - Diego Lara de Andres (Office for National Statistics)
 - Zoe White (Office for National Statistics)

## Dedication

This project is dedicated to the memory of Harvey Goldstein (1939-2020).

## License

Unless stated otherwise, the codebase is released under the [MIT License][mit]. This covers both the codebase and any sample code in the documentation.

The documentation is [© Crown copyright][copyright] and available under the terms of the [Open Government 3.0 license][ogl].

[copyright]: http://www.nationalarchives.gov.uk/information-management/re-using-public-sector-information/uk-government-licensing-framework/crown-copyright/
[guidance]: https://www.gov.uk/government/publications/joined-up-data-in-government-the-future-of-data-linking-methods
[mit]: LICENSE
[ogl]: http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/
[scalelink]: https://onlinelibrary.wiley.com/doi/10.1002/sim.7287
[user-guide]: docs/user_guide.md
