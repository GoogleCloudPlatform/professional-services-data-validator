# Copyright 2020 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import ibis.expr.types as tp


class CLOBValue(tp.StringValue):
    pass


class CLOBScalar(tp.StringScalar, CLOBValue):
    pass


class CLOBColumn(tp.StringColumn, CLOBValue):
    pass


class BLOBValue(tp.StringValue):
    pass


class BLOBScalar(tp.StringScalar, BLOBValue):
    pass


class BLOBColumn(tp.StringColumn, BLOBValue):
    pass


class DBCLOBValue(tp.BinaryValue):
    pass


class DBCLOBScalar(tp.BinaryScalar, DBCLOBValue):
    pass


class DBCLOBColumn(tp.BinaryColumn, DBCLOBValue):
    pass


# --------------Graphic-----------------------------


class GRAPHICValue(tp.StringValue):
    pass


class GRAPHICScalar(tp.StringScalar, GRAPHICValue):
    pass


class GRAPHICColumn(tp.StringColumn, GRAPHICValue):
    pass


# --------------Vargraphic-----------------------------


class VARGRAPHICValue(tp.StringValue):
    pass


class VARGRAPHICScalar(tp.StringScalar, VARGRAPHICValue):
    pass


class VARGRAPHICColumn(tp.StringColumn, VARGRAPHICValue):
    pass


# ---------------Binary Integer----------------------------


class SMALLINTValue(tp.NumericValue):
    pass


class SMALLINTScalar(tp.NumericScalar, SMALLINTValue):
    pass


class SMALLINTColumn(tp.NumericColumn, SMALLINTValue):
    pass


class INTEGERValue(tp.NumericValue):
    pass


class INTEGERScalar(tp.NumericScalar, INTEGERValue):
    pass


class INTEGERColumn(tp.NumericColumn, INTEGERValue):
    pass


class BIGINTValue(tp.NumericValue):
    pass


class BIGINTScalar(tp.NumericScalar, BIGINTValue):
    pass


class BIGINTColumn(tp.NumericColumn, BIGINTValue):
    pass


# -------------Floating points------------------------------


class REALValue(tp.NumericValue):
    pass


class REALScalar(tp.NumericScalar, REALValue):
    pass


class REALColumn(tp.NumericColumn, REALValue):
    pass
