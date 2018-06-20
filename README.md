# MemScrimper
This repository contains the code for the DIMVA 2018 paper: "MemScrimper: Time- and Space-Efficient Storage of Malware Sandbox Memory Dumps". Please note that in its current state, the code is a PoC and not a full-fledged production-ready application.

# Abstract
MemScrimper is a a novel methodology to compress memory dumps of malware sandboxes. MemScrimper is built on the observation that sandboxes always start at the same system state (i.e., a sandbox snapshot) to analyze malware. Therefore, memory dumps taken after malware execution inside the same sandbox are substantially similar to each other, which we can use to only store the differences introduced by the malware itself. Technically, we compare the pages of those memory dumps against the pages of a reference memory dump taken from the same sandbox and then deduplicate identical or similar pages accordingly. MemScrimper increases data compression ratios by up to 3894.74% compared to standard compression utilities such as `7zip`, and reduces compression and decompression times by up to 72.48% and 41.44, respectively. Furthermore, MemScrimper's internal storage allows to perform analyses (e.g., signature matching) on compressed memory dumps more efficient than on uncompressed dumps. MemScrimper thus significantly increases the retention time of memory dumps and makes longitudinal analysis more viable, while also improving efficiency.

# Paper
The paper is available [here](https://christian-rossow.de/publications/memscrimper-dimva2018.pdf). You can cite it with the following BibTeX entry:
```
@inproceedings{MemScrimper,
  author    = {Brengel, Michael and Rossow, Christian},
  title     = {{\textsc{MemScrimper}: Time- and Space-Efficient Storage of Malware Sandbox Memory Dumps}},
  booktitle = {Proceedings of the Conference on Detection of Intrusions and Malware, and Vulnerability Assessment~(DIMVA)},
  year      = {2018}
}
```

# Interested in more of our research?
[Come visit us](http://syssec.mmci.uni-saarland.de/).
