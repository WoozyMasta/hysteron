// Copyright 2026 WoozyMasta
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

// TimelineDivergenceReason identifies why two timeline states are considered
// on different branches.
type TimelineDivergenceReason string

const (
	// TimelineDivergenceNone reports no detected divergence.
	TimelineDivergenceNone TimelineDivergenceReason = "none"
	// TimelineDivergenceFollowedTimelineOlder reports followed timeline is older.
	TimelineDivergenceFollowedTimelineOlder TimelineDivergenceReason = "followed_timeline_older"
	// TimelineDivergenceSameTimelineDifferentSwitchPoint reports same timeline id
	// with different parent switch points.
	TimelineDivergenceSameTimelineDifferentSwitchPoint TimelineDivergenceReason = "same_timeline_different_switch_point"
	// TimelineDivergenceFollowedForkedBeforeCurrentPosition reports followed
	// timeline forked before the current XLogPos on the local timeline.
	TimelineDivergenceFollowedForkedBeforeCurrentPosition TimelineDivergenceReason = "followed_forked_before_current_position"
)

// TimelineBranchDivergence contains the result of branch-divergence detection.
type TimelineBranchDivergence struct {
	Reason              TimelineDivergenceReason
	FollowedSwitchPoint uint64
	CurrentSwitchPoint  uint64
	Different           bool
}

// DetectTimelineBranchDivergence checks whether followed timeline state belongs
// to a different branch from current timeline state.
func DetectTimelineBranchDivergence(
	followedTimelineID uint64,
	followedTimelineHistory PostgresTimelinesHistory,
	followedXLogPos uint64,
	currentTimelineID uint64,
	currentTimelineHistory PostgresTimelinesHistory,
	currentXLogPos uint64,
) TimelineBranchDivergence {
	if followedTimelineID < currentTimelineID {
		return TimelineBranchDivergence{
			Different: true,
			Reason:    TimelineDivergenceFollowedTimelineOlder,
		}
	}

	// If timelines are the same, compare the parent switch point.
	if followedTimelineID == currentTimelineID {
		if currentTimelineID <= 1 {
			// No timeline history exists for timeline <= 1.
			return TimelineBranchDivergence{Reason: TimelineDivergenceNone}
		}
		followedHistory := followedTimelineHistory.GetTimelineHistory(currentTimelineID - 1)
		currentHistory := currentTimelineHistory.GetTimelineHistory(currentTimelineID - 1)
		if followedHistory == nil || currentHistory == nil {
			return TimelineBranchDivergence{Reason: TimelineDivergenceNone}
		}
		if followedHistory.SwitchPoint != currentHistory.SwitchPoint {
			return TimelineBranchDivergence{
				Different:           true,
				Reason:              TimelineDivergenceSameTimelineDifferentSwitchPoint,
				FollowedSwitchPoint: followedHistory.SwitchPoint,
				CurrentSwitchPoint:  currentHistory.SwitchPoint,
			}
		}
		return TimelineBranchDivergence{Reason: TimelineDivergenceNone}
	}

	// followedTimelineID > currentTimelineID
	followedHistory := followedTimelineHistory.GetTimelineHistory(currentTimelineID)
	if followedHistory != nil && followedHistory.SwitchPoint < currentXLogPos {
		return TimelineBranchDivergence{
			Different:           true,
			Reason:              TimelineDivergenceFollowedForkedBeforeCurrentPosition,
			FollowedSwitchPoint: followedHistory.SwitchPoint,
			CurrentSwitchPoint:  currentXLogPos,
		}
	}

	_ = followedXLogPos // kept for future extension without changing signature.
	return TimelineBranchDivergence{Reason: TimelineDivergenceNone}
}
