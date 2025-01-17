import random
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Date, Float
from python_scripts.database_operations import Database


class StudentActivityGenerator:
    def __init__(self, db_user, db_pass, db_host, db_port, db_name):
        """
        Initialize the database connection and activities DataFrame.
        """
        self.db = Database(db_user, db_pass, db_host, db_port, db_name)
        self.activities = pd.DataFrame()  # Initialize an empty DataFrame to hold activities
        self.student_account_map = {}  # Map to ensure the same student always has the same account

    def fetch_google_trends(self):
        """
        Fetch and normalize Google Trends data for a specific keyword.
        """
        query = """
        SELECT DATE, FREQUENCY
        FROM google_trends
        ORDER BY DATE;
        """
        try:
            trends_df = self.db.fetch_query(query)
            return trends_df
        except Exception as e:
            print(f"Failed to fetch Google Trends data: {e}")
            raise

    def generate_student_activities(self, trends_df, student_ids, account_ids):
        """
        Generate student activities based on Google Trends data and store them in self.activities.

        Args:
            trends_df (pd.DataFrame): Trends data with 'date' and normalized 'frequency'.
            student_ids (list): List of student IDs (sample of 100 students).
            account_ids (list): List of account IDs.
        """
        activities = []

        # pre-assign accounts to students to ensure consistency
        self.student_account_map = {
            student_id: random.choice(account_ids + [0]) for student_id in student_ids
        }

        # create high and low performing student groups
        high_performers = random.sample(student_ids, int(0.3 * len(student_ids)))  # top 30%  ofstudents
        low_performers = random.sample([s for s in student_ids if s not in high_performers], int(0.2 * len(student_ids)))  # bottom 20% students

        for _, row in trends_df.iterrows():
            # determine number of active students based on normalized frequency
            active_students_count = int(row["frequency"])
            active_students = random.sample(student_ids, min(active_students_count, len(student_ids)))

            # generate interactions for each active student
            for student_id in active_students:
                # always use the pre-assigned account ID for this student
                account_id = self.student_account_map[student_id]

                # random number of interactions per student (mean 2, standard deviation of 1.25)
                num_interactions = max(1, int(random.normalvariate(2, 1.25)))

                for _ in range(num_interactions):
                    resource_type = "movie" if random.random() < 0.6 else "quiz"

                    if resource_type == "quiz":
                        score = self._generate_quiz_score(student_id, high_performers, low_performers)
                        quiz_id = random.randint(50, 100)
                    else:
                        score = 0
                        quiz_id = None

                    activities.append({
                        "id": random.randint(10000000, 11000000),  # Randomly generate ID
                        "date": row["date"],
                        "student_id": student_id,
                        "account_id": account_id,
                        "resource_type": resource_type,
                        "score": score,
                        "quiz_id": quiz_id,
                        "etl_last_updated_ts": pd.to_datetime(row["date"])  # Ensure timestamp is always set
                    })

        # Apply quiz-after-movie logic
        self.activities = pd.DataFrame(self._prioritize_quizzes_after_movies(activities))
        # Mimic data duplication
        self.activities = self._mimic_data_duplication(self.activities)

    def _generate_quiz_score(self, student_id, high_performers, low_performers):
        """
        Generate a realistic quiz score with a mean of 7, adjusting for high and low performers.

        Args:
            student_id (int): ID of the student taking the quiz.
            high_performers (list): List of high-performing students.
            low_performers (list): List of low-performing students.

        Returns:
            int: A score between 0 and 10.
        """
        if student_id in high_performers:
            score = min(max(int(random.normalvariate(8.5, 1)), 7), 10)  # High performers mostly score 7-10
        elif student_id in low_performers:
            score = min(max(int(random.normalvariate(4, 2)), 0), 6)  # Low performers mostly score 0-6
        else:
            score = min(max(int(random.normalvariate(7, 2)), 0), 10)  # Average performers
        return score

    def _prioritize_quizzes_after_movies(self, activities):
        """
        Adjust activities to prioritize quizzes following movies.
        """
        movie_dates = {a["date"] for a in activities if a["resource_type"] == "movie"}
        updated_activities = []

        for activity in activities:
            if activity["resource_type"] == "quiz":
                if random.random() < 0.7 and movie_dates:  # 70% chance quiz follows a movie
                    activity["date"] = random.choice(list(movie_dates))
            updated_activities.append(activity)

        return updated_activities

    def _mimic_data_duplication(self, activities_df):
        """
        Mimic data duplication by creating duplicate records with account_id set to 0 for duplicates.

        Args:
            activities_df (pd.DataFrame): Original activities DataFrame.

        Returns:
            pd.DataFrame: DataFrame with duplicate records added.
        """
        duplication_rate = 0.3  # 30% of rows will have duplicates
        num_duplicates = int(len(activities_df) * duplication_rate)

        duplicates = activities_df.sample(n=num_duplicates, replace=True)
        duplicates["account_id"] = 0  # Set account_id to 0 for duplicates
        duplicates["etl_last_updated_ts"] = duplicates["etl_last_updated_ts"] - pd.to_timedelta(random.randint(1, 10), unit="s")

        duplicated_df = pd.concat([activities_df, duplicates]).reset_index(drop=True)
        return duplicated_df

    def save_activities_to_db(self):
        """
        Save generated activities to the stg_student_activities table in the database.
        """
        if self.activities.empty:
            print("No activities to save.")
            return

        try:
            self.activities.drop(columns=["date"], inplace=True)  # Remove temporary date column
            self.db.load_dataframe(self.activities, "stg_student_activities")
            print("Student activities saved to database.")
        except Exception as e:
            print(f"Failed to save activities: {e}")
            raise

    def close(self):
        """
        Close the database connection.
        """
        self.db.close()


