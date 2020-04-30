package cs223.project1;

import java.io.IOException;
import java.sql.SQLException;


public class Project1Application {
	public static void main(String[] args) throws SQLException, IOException {

		Configuration config = new Configuration();

		// TODO: 2/11/2020 Consistency
		Simulator simulator = new Simulator(config);
		simulator.simulate();

	}

}


