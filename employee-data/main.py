import pandas as pd
from dataclasses import dataclass
from datetime import  date, datetime

@dataclass
class Employee:
    name: str
    employee_id: str
    department: str
    salary: int
    hire_date: date

    def display(self) -> str:
        return f"Name: {self.name}\nId: {self.employee_id}\nDepartment {self.department}\nWork since {self.hire_date}"
    
    def update_salary(self) -> None:
        self.salary *= 1.1

    def eligible_for_bonus(self) -> bool:
        today = datetime.now().date()
        diff = today - self.hire_date
        if diff.days/365 < 5:
            return False
        return True
    
@dataclass
class Department:
    employees: list[Employee]
    name: str

    def add_employee(self, employee: Employee) -> None:
        self.employees.append(employee)

    def calculate_avg_salary(self) -> float:
        salaries = [employee.salary for employee in self.employees]
        return sum(salaries)/len(salaries)
    
    def report(self) -> str:
        total_salary = sum([employee.salary for employee in self.employees])
        return f"Department name: {self.name}\nTotal employees: {len(self.employees)}\nAVG salary: {self.calculate_avg_salary()}\nTOTAL salary: {total_salary}\n\n"
    
    def eligible_for_bonus(self) -> list[Employee]|None:
        return [employee.eligible_for_bonus() for employee in self.employees]


def main():
    departments = dict()
    # test_user = Employee("Bob", 5555, "Design", 6000, hire_date=date(2015, 12, 5))
    # print(test_user.display())
    # print(test_user.eligible_for_bonus())
    df = pd.read_csv("test.csv")
    for index, row in df.iterrows():

        hire_date = datetime.strptime(row['hire_date'], '%Y-%m-%d').date()
        employee = Employee(
        name=row['name'],
        employee_id=row['employee_id'],
        department=row['department'],
        salary=row['salary'],
        hire_date=hire_date
        )

        if employee.department in departments:
            departments[employee.department].add_employee(employee)
        else:
            departments[employee.department] = Department(employees=[employee], name=employee.department)

    for _, department in departments.items():
        print(department.report())

if __name__ == "__main__":
    main()
