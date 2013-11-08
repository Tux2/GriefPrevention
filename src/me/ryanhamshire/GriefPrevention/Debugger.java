package me.ryanhamshire.GriefPrevention;

public class Debugger {

    public static void setDebugLevel(DebugLevel debugLevel) {
        GriefPrevention.instance.debug.setCurrentLevel(debugLevel);
    }

    public void setCurrentLevel(DebugLevel currentLevel) {
        this.CurrentLevel = currentLevel;
    }

    public enum DebugLevel {
		Errors, Informational, None, Verbose, Warning;

		/**
		 * returns whether the given DebugLevel applies to this one.
		 * 
		 * @param checkapply
		 * @return
		 */
		public boolean applies(DebugLevel checkapply) {
			return ordinal() <= checkapply.ordinal();
		}
	}

	public static DebugLevel getCurrentDebugLevel() {
		return GriefPrevention.instance.debug.getCurrentLevel();
	}

	public static void Write(String Message, DebugLevel Level) {
		GriefPrevention instance = GriefPrevention.instance;
		if (instance != null && instance.debug != null) {
			instance.debug.Output(Message, Level);
		}
	}

	private DebugLevel CurrentLevel;

	public Debugger(DebugLevel DebuggingLevel) {

		GriefPrevention.AddLogEntry("Debug Message Granularity set to " + DebuggingLevel.name());
		GriefPrevention.AddLogEntry("To change Debug Message granularity, edit the \"GriefPrevention.DebugLevel\" Setting in config.yml.");

		// GriefPrevention.AddLogEntry("Debug Message Granularity:" +
		// DebuggingLevel.name());
		CurrentLevel = DebuggingLevel;
	}

	public DebugLevel getCurrentLevel() {
		return CurrentLevel;
	}

	public void Output(String Message, DebugLevel Level) {
        if (CurrentLevel == DebugLevel.None)
			return;
		if (CurrentLevel.applies(Level)) {
			GriefPrevention.AddLogEntry("[" + Level.name() + "]:" + Message);
		}
	}

}
