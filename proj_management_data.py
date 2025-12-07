import random
import uuid
from datetime import datetime, timedelta

import pandas as pd

# ------------ Config ------------
N_PROJECTS = 10
TASKS_PER_PROJECT_MIN = 20
TASKS_PER_PROJECT_MAX = 60
RANDOM_SEED = 42
# --------------------------------

random.seed(RANDOM_SEED)

PROJECT_STATUSES = ["On Track", "At Risk", "Delayed", "Completed"]
TASK_STATUSES = ["Not Started", "In Progress", "Blocked", "Completed"]
PRIORITIES = ["Low", "Medium", "High", "Critical"]
ROLES = ["Developer", "Analyst", "PM", "QA", "DevOps"]

def random_date(start: datetime, end: datetime) -> datetime:
    """Pick a random datetime between start and end."""
    delta = end - start
    offset_days = random.randint(0, delta.days)
    return start + timedelta(days=offset_days)

def generate_projects(n_projects: int):
    projects = []
    base_start = datetime(2024, 1, 1)
    base_end = datetime(2026, 12, 31)

    for i in range(n_projects):
        project_id = str(uuid.uuid4())
        start_date = random_date(base_start, base_end - timedelta(days=180))
        end_date = start_date + timedelta(days=random.randint(60, 365))
        status = random.choices(
            PROJECT_STATUSES,
            weights=[0.5, 0.2, 0.2, 0.1],  # more "On Track"
            k=1
        )[0]
        budget = random.randint(200_000, 5_000_000)

        projects.append({
            "project_id": project_id,
            "project_name": f"Project {i+1}",
            "owner": f"PM_{i+1}",
            "status": status,
            "start_date": start_date.date().isoformat(),
            "end_date": end_date.date().isoformat(),
            "budget_usd": budget
        })

    return pd.DataFrame(projects)

def generate_tasks(projects_df: pd.DataFrame):
    tasks = []
    task_counter = 1

    for _, row in projects_df.iterrows():
        n_tasks = random.randint(TASKS_PER_PROJECT_MIN, TASKS_PER_PROJECT_MAX)
        project_start = datetime.fromisoformat(row["start_date"])
        project_end = datetime.fromisoformat(row["end_date"])

        for _ in range(n_tasks):
            task_id = str(uuid.uuid4())
            assignee_id = random.randint(1, 30)
            assignee = f"User_{assignee_id}"
            role = random.choice(ROLES)

            start_date = random_date(project_start, project_end - timedelta(days=7))
            duration_days = random.randint(3, 45)
            planned_end = start_date + timedelta(days=duration_days)

            # introduce random delays
            delay_days = max(0, int(random.gauss(0, 3)))
            actual_end = planned_end + timedelta(days=delay_days)

            status = random.choices(
                TASK_STATUSES,
                weights=[0.2, 0.4, 0.1, 0.3],
                k=1
            )[0]

            priority = random.choices(
                PRIORITIES,
                weights=[0.3, 0.4, 0.2, 0.1],
                k=1
            )[0]

            tasks.append({
                "task_id": task_id,
                "task_name": f"Task {task_counter}",
                "project_id": row["project_id"],
                "assignee": assignee,
                "role": role,
                "status": status,
                "priority": priority,
                "start_date": start_date.date().isoformat(),
                "planned_end_date": planned_end.date().isoformat(),
                "actual_end_date": actual_end.date().isoformat(),
                "estimated_hours": random.randint(4, 160),
                "logged_hours": max(0, int(random.gauss(40, 20))),
                "delay_days": delay_days
            })
            task_counter += 1

    return pd.DataFrame(tasks)

def main():
    projects_df = generate_projects(N_PROJECTS)
    tasks_df = generate_tasks(projects_df)

    print("Projects sample:")
    print(projects_df.head())
    print("\nTasks sample:")
    print(tasks_df.head())

    # Save to CSV for Foundry upload / pipelines
    projects_df.to_csv("projects_mock.csv", index=False)
    tasks_df.to_csv("tasks_mock.csv", index=False)
    print("\nSaved projects_mock.csv and tasks_mock.csv")

if __name__ == "__main__":
    main()
