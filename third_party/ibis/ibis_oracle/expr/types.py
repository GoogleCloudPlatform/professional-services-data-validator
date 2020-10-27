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
    pass  # noqa: E701,E302


class CLOBScalar(tp.StringScalar, CLOBValue):
    pass  # noqa: E701,E302


class CLOBColumn(tp.StringColumn, CLOBValue):
    pass  # noqa: E701,E302


# -------------------------------------------


class NCLOBValue(tp.StringValue):
    pass  # noqa: E701,E302


class NCLOBScalar(tp.StringScalar, NCLOBValue):
    pass  # noqa: E701,E302


class NCLOBColumn(tp.StringColumn, NCLOBValue):
    pass  # noqa: E701,E302


# -------------------------------------------


class LONGValue(tp.StringValue):
    pass  # noqa: E701,E302


class LONGScalar(tp.StringScalar, LONGValue):
    pass  # noqa: E701,E302


class LONGColumn(tp.StringColumn, LONGValue):
    pass  # noqa: E701,E302


# --------------Binary type-----------------------------


class BFILEValue(tp.BinaryValue):
    pass  # noqa: E701,E302


class BFILEScalar(tp.BinaryScalar, BFILEValue):
    pass  # noqa: E701,E302


class BFILEColumn(tp.BinaryColumn, BFILEValue):
    pass  # noqa: E701,E302


# -------------------------------------------


class RAWValue(tp.BinaryValue):
    pass  # noqa: E701,E302


class RAWScalar(tp.BinaryScalar, RAWValue):
    pass  # noqa: E701,E302


class RAWColumn(tp.BinaryColumn, RAWValue):
    pass  # noqa: E701,E302


# -------------------------------------------


class LONGRAWValue(tp.BinaryValue):
    pass  # noqa: E701,E302


class LONGRAWScalar(tp.BinaryScalar, LONGRAWValue):
    pass  # noqa: E701,E302


class LONGRAWColumn(tp.BinaryColumn, LONGRAWValue):
    pass  # noqa: E701,E302


# -------------Number type------------------------------


class NumberValue(tp.NumericValue):
    pass  # noqa: E701,E302


class NumberScalar(tp.NumericScalar, NumberValue):
    pass  # noqa: E701,E302


class NumberColumn(tp.NumericColumn, NumberValue):
    pass  # noqa: E701,E302
