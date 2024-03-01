# The MIT License (MIT)
# Copyright (c) 2023 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import os

SMOS_DATA_STORE_ID = "smos"

INDEX_ENV_VAR_NAME = "XCUBE_SMOS_INDEX_PATH"

OS_VAR_NAMES = {
    "Mean_acq_time",
    "SSS_corr",
    "Sigma_SSS_corr",
    "SSS_anom",
    "Sigma_SSS_anom",
    "Dg_chi2_corr",
    "Dg_quality_SSS_corr",
    "Dg_quality_SSS_anom",
    "Coast_distance",
    "Dg_RFI_X",
    "Dg_RFI_Y",
    "X_swath",
}

SM_VAR_NAMES = {
    "Mean_acq_time",
    "Soil_Moisture",
    "Soil_Moisture_DQX",
    "Chi_2",
    "Chi_2_P",
    "N_RFI_X",
    "N_RFI_Y",
    "RFI_Prob",
    "X_swath",
}

SM_DATA_ID = "SMOS-L2C-SM"
OS_DATA_ID = "SMOS-L2C-OS"

DATASET_VAR_NAMES = {SM_DATA_ID: SM_VAR_NAMES, OS_DATA_ID: OS_VAR_NAMES}

DATASET_ATTRIBUTES = {
    SM_DATA_ID: {"title": "SMOS Level-2 Soil Moisture"},
    OS_DATA_ID: {"title": "SMOS Level-2 Ocean Salinity"},
}

DEFAULT_STAC_URL = "https://datahub.creodias.eu/stac/collections/SMOS"
DEFAULT_ENDPOINT_URL = "https://s3.cloudferro.com"
DEFAULT_ARCHIVE_URL = "s3://EODATA"

DEFAULT_STORAGE_OPTIONS = {
    "endpoint_url": DEFAULT_ENDPOINT_URL,
    "anon": False,
    "key": os.environ.get("CREODIAS_S3_KEY"),
    "secret": os.environ.get("CREODIAS_S3_SECRET"),
}
