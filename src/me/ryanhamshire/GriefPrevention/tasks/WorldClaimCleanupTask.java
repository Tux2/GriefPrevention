package me.ryanhamshire.GriefPrevention.tasks;

import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import me.ryanhamshire.GriefPrevention.Claim;
import me.ryanhamshire.GriefPrevention.Debugger;
import me.ryanhamshire.GriefPrevention.Debugger.DebugLevel;
import me.ryanhamshire.GriefPrevention.GriefPrevention;
import me.ryanhamshire.GriefPrevention.PlayerData;
import me.ryanhamshire.GriefPrevention.Configuration.WorldConfig;

import org.bukkit.Chunk;
import org.bukkit.World;

public class WorldClaimCleanupTask implements Runnable {
    private String CleanupWorldName;
    private boolean flInitialized = false;
    // Claim Cleanup has/will be refactored to working
    // per world rather than globally.
    // This decision was made because the previous logic was too unpredictable.
    private int nextClaimIndex = 0;
    private int TaskCookie = 0;
    private WorldConfig wc = null;
    public int lastcleaned = 0;

    public WorldClaimCleanupTask(String WorldCleanup) {
	CleanupWorldName = WorldCleanup;
	wc = GriefPrevention.instance.getWorldCfg(WorldCleanup);
	Debugger.Write("WorldClaimCleanupTask started for world:" + WorldCleanup, DebugLevel.Verbose);
    }

    public int getTaskCookie() {
	return TaskCookie;
    }

    public void run(){

        // retrieve the claims mapped to our world.
        List<Claim> WorldClaims = GriefPrevention.instance.dataStore.getClaimArray().getWorldClaims(CleanupWorldName);

        // if the list is null or empty, we have no work to do, so return out.
        if (WorldClaims == null || WorldClaims.size() == 0) return;

        // Get todays date and do the math to prepare claims for expiration
        // Calculate how long ago a player can have last logged in for the three expiration types (chest claims, all claims, and unused claims)
        Calendar chestClaimLastValidLogin = Calendar.getInstance();
        Calendar allClaimsLastValidLogin = Calendar.getInstance();
        Calendar unusedClaimsLastValidLogin = Calendar.getInstance();
        chestClaimLastValidLogin.add(Calendar.DATE, -wc.getChestClaimExpirationDays());
        allClaimsLastValidLogin.add(Calendar.DATE, -wc.getClaimsExpirationDays());
        unusedClaimsLastValidLogin.add(Calendar.DATE, -wc.getUnusedClaimExpirationDays());
        
        // Initialize flag for unloading chunks from memory if nature restoration is enabled
        boolean cleanupChunks = false;

        // Start with first element in WorldClaims array and cycle through them all to check for expirations
        for ( int claimIndex = 0; claimIndex < WorldClaims.size(); claimIndex++) {
            Claim claim = WorldClaims.get(claimIndex);
            // skip administrative claims
            if (claim.isAdminClaim())
                continue;

            // get data for the player, especially last login timestamp
            PlayerData playerData = GriefPrevention.instance.dataStore.getPlayerData(claim.getOwnerName());
    
            // Confirm player hasn't played in the required time for their various claims
            boolean chestClaimExpired = chestClaimLastValidLogin.getTime().after(playerData.lastLogin);
            boolean allClaimsExpired = allClaimsLastValidLogin.getTime().after(playerData.lastLogin);
            boolean unusedClaimsExpired = unusedClaimsLastValidLogin.getTime().after(playerData.lastLogin);
            // A special case for unused or under-used claims; creative rules implies that a claim is ALWAYS checked for under-utilization
            boolean creativeRules = GriefPrevention.instance.creativeRulesApply(claim.getLesserBoundaryCorner());
            
            // Chest claim expiration runs only if it is the player's only claim; otherwise, skip to the more thorough all claims expiration step
            if (chestClaimExpired && playerData.claims.size() == 1 && wc.getChestClaimExpirationDays() > 0) {
                // Determine area of the default chest claim; claims bigger than this imply the player has editted the claim
                // These claims will instead be handled by the all claims expiration
                // In the event the chest claim radius is defined < 0, abort this check -- why a negative claim radius?
                int areaOfDefaultClaim = 0;
                if (wc.getAutomaticClaimsForNewPlayerRadius() >= 0) {
                    areaOfDefaultClaim = (int) Math.pow(wc.getAutomaticClaimsForNewPlayerRadius() * 2 + 1, 2);
                }
                else {
                    Debugger.Write("Seems the default chest claim radius is set < 0; claim cleanup will not work on these claims!", DebugLevel.Verbose);
                    continue;
                }
                if (claim.getArea() > areaOfDefaultClaim ) {
                    continue;
                }

                Debugger.Write("Deleting Chest Claim owned by " + claim.getOwnerName() + " last login:" + playerData.lastLogin.toString(), DebugLevel.Verbose);
                claim.removeSurfaceFluids(null);
                GriefPrevention.instance.dataStore.deleteClaim(claim);
                lastcleaned++;
                if (playerData.claims.size() == 0) {
                    GriefPrevention.instance.dataStore.deletePlayerData(playerData.playerName);
                }

                // if configured to do so, restore the land to natural
                if (wc.getClaimsAutoNatureRestoration()) {
                    GriefPrevention.instance.restoreClaim(claim, 0);
                    // Set this flag to true so any chunks loaded by this process will be properly unloaded at the end of the process
                    cleanupChunks = true;
                }
                GriefPrevention.AddLogEntry(" " + claim.getOwnerName() + "'s chest claim expired.");
			
            }
            else if (allClaimsExpired && wc.getClaimsExpirationDays() > 0) {
		// make a copy of this player's claim list
		Vector<Claim> claims = new Vector<Claim>();
		for (int i = 0; i < playerData.claims.size(); i++) {
                    claims.add(playerData.claims.get(i));
		}

                // delete them
		GriefPrevention.instance.dataStore.deleteClaimsForPlayer(claim.getOwnerName(), true, false);
		GriefPrevention.AddLogEntry(" All of " + claim.getOwnerName() + "'s claims have expired. Removing all but the locked claims.");
		GriefPrevention.instance.dataStore.deletePlayerData(playerData.playerName);

		for (int i = 0; i < claims.size(); i++) {
                    // if configured to do so, restore the land to natural
                    if (wc.getClaimsAutoNatureRestoration()) {
                        GriefPrevention.instance.restoreClaim(claims.get(i), 0);
                        // Set this flag to true so any chunks loaded by this process will be properly unloaded at the end of the process
			cleanupChunks = true;
                    }
                }
            }
            else if ( (unusedClaimsExpired && wc.getUnusedClaimExpirationDays() > 0) || creativeRules ) {

		// avoid scanning large claims, locked claims, and administrative claims
                // Claims not scanned due to their size are still subject to the all claims expiration
                boolean sizelimitreached = (claim.getWidth() > wc.getClaimCleanupMaximumSize());
		if (claim.isAdminClaim() || claim.neverdelete || sizelimitreached)
                    continue;

		int minInvestment = wc.getClaimCleanupMaxInvestmentScore();
		// if minInvestment is 0, assume no limitation and force the
		// following conditions to clear the claim.
		long investmentScore = minInvestment == 0 ? Long.MAX_VALUE : claim.getPlayerInvestmentScore();
		cleanupChunks = true;
		boolean removeClaim = false;

		// in creative mode, a build which is almost entirely lava above
                // sea level will be automatically removed, even if the owner is
		// an active player
		// lava above the surface deducts 10 points per block from the
		// investment score
		// so 500 blocks of lava without anything built to offset all
		// that potential mess would be cleaned up automatically
		if (GriefPrevention.instance.creativeRulesApply(claim.getLesserBoundaryCorner()) && investmentScore < -5000) {
                    Debugger.Write("Creative Rules World, InvestmentScore of " + investmentScore + " is below -5000", DebugLevel.Verbose);
                    removeClaim = true;
		}

		// otherwise, the only way to get a claim automatically removed
		// based on build investment is to be away for the configured time AND not
		// build much of anything
		else if (investmentScore < minInvestment) {
                    Debugger.Write("Investment Score (" + investmentScore + " does not meet threshold " + minInvestment, DebugLevel.Verbose);
                    removeClaim = true;
		}

		if (removeClaim) {
                    GriefPrevention.instance.dataStore.deleteClaim(claim);
                    GriefPrevention.AddLogEntry("Removed " + claim.getOwnerName() + "'s unused claim @ " + GriefPrevention.getfriendlyLocationString(claim.getLesserBoundaryCorner()));

                    // if configured to do so, restore the claim area to natural
                    // state
                    if (wc.getClaimsAutoNatureRestoration()) {
			GriefPrevention.instance.restoreClaim(claim, 0);
                    }
                }
            }

            // toss that player data out of the cache, it's probably not needed in
            // memory right now
            if (!GriefPrevention.instance.getServer().getOfflinePlayer(claim.getOwnerName()).isOnline()) {
		GriefPrevention.instance.dataStore.clearCachedPlayerData(claim.getOwnerName());
            }

            // since we're potentially loading a lot of chunks to scan parts of the
            // world where there are no players currently playing, be mindful of
            // memory usage
            if (cleanupChunks) {
                World world = claim.getLesserBoundaryCorner().getWorld();
                Chunk[] chunks = world.getLoadedChunks();
		for (int i = chunks.length - 1; i > 0; i--) {
                    Chunk chunk = chunks[i];
                    chunk.unload(true, true);
		}
            }
        }
    }

    public void setTaskCookie(int value) {
	TaskCookie = value;
    }
}
