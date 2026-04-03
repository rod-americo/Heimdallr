Heimdallr
Copyright (c) 2026 Rodrigo Americo

This product includes software developed by the Heimdallr contributors.

================================================================================

Third-Party Dependencies and Notices
================================================================================

This software uses the following third-party components, each subject to their
own licenses:

TotalSegmentator
----------------
TotalSegmentator is used for medical image segmentation. TotalSegmentator
requires a valid license for commercial use. For more information, visit:
https://github.com/wasserth/TotalSegmentator

Users of Heimdallr must ensure compliance with TotalSegmentator's licensing
requirements independently.

Python Dependencies
-------------------
This software depends on various Python packages listed in requirements.txt,
including but not limited to:
- FastAPI (MIT License)
- PyDICOM (MIT License)
- NiBabel (MIT License)
- NumPy (BSD License)
- PyTorch (BSD License)

For a complete list of dependencies and their licenses, see requirements.txt
and the licenses directory in the virtual environment (venv/lib/pythonX.X/
site-packages/*/LICENSE or *.dist-info/licenses/).

dcm2niix
--------
This software uses dcm2niix for DICOM to NIfTI conversion. dcm2niix is
licensed under the BSD 2-Clause License.

ChestXRayAnatomySegmentation (CXAS)
------------------------------------
The CTR (Cardiothoracic Ratio / ICT) extraction module (ctr_api.py) is
based on ChestXRayAnatomySegmentation by Constantin Seibold et al.
Licensed under Creative Commons Attribution-NonCommercial-ShareAlike 4.0
International (CC BY-NC-SA 4.0).
https://github.com/ConstantinSeibold/ChestXRayAnatomySegmentation

This integration is used for personal and experimental purposes only.
No commercial distribution or monetization is permitted under this license.
Derivatives must carry the same CC BY-NC-SA 4.0 license.

MedGemma
--------
The MedGemma Analysis Service (medgemma_api.py) uses Google MedGemma
(google/medgemma-1.5-4b-it), governed by the Health AI Developer
Foundations Terms of Use.
https://developers.google.com/health-ai-developer-foundations/terms

MedGemma is not an approved medical device.  All outputs are assistive
and must be reviewed by qualified professionals.
