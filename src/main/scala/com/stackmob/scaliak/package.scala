/**
 * Copyright 2012-2013 StackMob
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stackmob

import java.util.Date
import com.basho.riak.client.api.cap.VClock

package object scaliak {

  implicit def boolToAllowSiblingsArg(b: Boolean): AllowSiblingsArgument = AllowSiblingsArgument(Option(b))
  implicit def boolToLastWriteWinsArg(b: Boolean): LastWriteWinsArgument = LastWriteWinsArgument(Option(b))
  implicit def intToNValArg(i: Int): NValArgument = NValArgument(Option(i))
  implicit def intToRARg(i: Int): RArgument = RArgument(Option(i))
  implicit def intToPRArg(i: Int): PRArgument = PRArgument(Option(i))
  implicit def boolToNotFoundOkArg(b: Boolean): NotFoundOkArgument = NotFoundOkArgument(Option(b))
  implicit def boolToBasicQuorumArg(b: Boolean): BasicQuorumArgument = BasicQuorumArgument(Option(b))
  implicit def boolToReturnDeletedVClockArg(b: Boolean): ReturnDeletedVCLockArgument = ReturnDeletedVCLockArgument(Option(b))
  implicit def vclockToIfModifiedVClockArg(v: Array[Byte]): IfModifiedVClockArgument = IfModifiedVClockArgument(Option(v))
  implicit def intToWArg(i: Int): WArgument = WArgument(Option(i))
  implicit def intToRWArg(i: Int): RWArgument = RWArgument(Option(i))
  implicit def intToDWArg(i: Int): DWArgument = DWArgument(Option(i))
  implicit def intToPWArg(i: Int): PWArgument = PWArgument(Option(i))
  implicit def boolToReturnBodyArg(b: Boolean): ReturnBodyArgument = ReturnBodyArgument(Option(b))

}
