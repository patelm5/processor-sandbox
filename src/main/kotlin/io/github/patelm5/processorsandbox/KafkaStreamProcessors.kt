package io.github.patelm5.processorsandbox

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.apache.kafka.streams.kstream.Reducer


@JsonIgnoreProperties(ignoreUnknown = true)
data class FootballStats(
    var currentPhase: String,
    var gameId: Int,
    var firstHalfHomeStats: FootballTeamStats = FootballTeamStats(),
    var secondHalfHomeStats: FootballTeamStats = FootballTeamStats(),
    var firstHalfAwayStats: FootballTeamStats = FootballTeamStats(),
    var secondHalfAwayStats: FootballTeamStats = FootballTeamStats(),
    var homeStats: FootballTeamStats = FootballTeamStats(),
    var awayStats: FootballTeamStats = FootballTeamStats()
    )


@JsonIgnoreProperties(ignoreUnknown = true)
data class FootballTeamStats(
    var goals: Int = 0,
    var yellowCards: Int = 0,
    var redCards: Int = 0,
    var offsides: Int = 0,
    var throwIns: Int = 0
)

fun FootballTeamStats.sub(other: FootballTeamStats) = this.copy(
    goals = this.goals - other.goals,
    yellowCards = this.yellowCards - other.yellowCards,
    redCards = this.redCards - other.redCards,
    offsides = this.offsides - other.offsides,
    throwIns = this.throwIns - other.throwIns
)

class FootballStatsReducer: Reducer<FootballStats> {
    override fun apply(agg: FootballStats, newMsg: FootballStats): FootballStats = agg.apply{
        gameId = newMsg.gameId
        homeStats = newMsg.homeStats
        awayStats = newMsg.awayStats
        currentPhase = newMsg.currentPhase
        when(newMsg.currentPhase){
            "FirstHalf" -> {
                firstHalfHomeStats = newMsg.homeStats
                firstHalfAwayStats = newMsg.awayStats
            }
            "SecondHalf" -> {
                secondHalfHomeStats = newMsg.homeStats.sub(agg.firstHalfHomeStats)
                secondHalfAwayStats = newMsg.awayStats.sub(agg.firstHalfAwayStats)
            }
        }
    }
}
