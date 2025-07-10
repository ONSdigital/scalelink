# Scalelink

An implementation of the probabilistic data linkage method **Scalelink**, described in [Goldstein *et al*. (2017)][scalelink].

This implementation has been created in response to recommendation six of the [Joined Up Data in Government: The Future of Data Linking Methods][guidance] cross-government review.

The Data Linkage team within the Methodology and Quality Directorate of the Office for National Statistics (ONS) have authored this work. This team researches novel data linkage methods and applications to facilitate production of high-quality linked datasets for national statistics.

`scalelink` is a Python and PySpark codebase built with Python 3.8-3.10 and Spark 2.4.0-3.2.3.

## Method

The Scalelink method is a novel probabilistic data linkage method. It uses a scaling algorithm based on correspondence analysis. A key potential advantage is that it utilises linkage variable dependence. In contrast, the current gold-standard probabilistic data linkage method, the Fellegi-Sunter algorithm, assumes that linkage variables are independent, an assumption which is violated to at least some extent in all linkage problems. For example, forenames are correlated with middle names, surnames, age, gender, home address, etc.

The Scalelink method is **experimental**. This implementation is has been open-sourced to facilitate further research regarding the suitability of the Scalelink method for real-world data linkage, particularly on Big Data. The authors **do not** currently endorse using the Scalelink method to produce linked datasets for research, analysis or statistics.

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
 - Zoe White (Office for National Statistics)

## License

Unless stated otherwise, the codebase is released under the [MIT License][mit]. This covers both the codebase and any sample code in the documentation.

The documentation is [© Crown copyright][copyright] and available under the terms of the [Open Government 3.0 license][ogl].

[scalelink]: https://onlinelibrary.wiley.com/doi/10.1002/sim.7287
[guidance]: https://www.gov.uk/government/publications/joined-up-data-in-government-the-future-of-data-linking-methods
[mit]: LICENSE
[copyright]: http://www.nationalarchives.gov.uk/information-management/re-using-public-sector-information/uk-government-licensing-framework/crown-copyright/
[ogl]: http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/
