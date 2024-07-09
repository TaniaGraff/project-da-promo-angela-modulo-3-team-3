CREATE Schema ABC_corporation;

USE ABC_corporation;

CREATE TABLE Employees (
    EmployeeNumber INT PRIMARY KEY AUTO_INCREMENT,
    Age INT,
    Gender VARCHAR(100),
    MaritalStatus VARCHAR(100),
    DateBirth DATE,
    Education INT,
    EducationField VARCHAR(100),
    Attrition VARCHAR(100),
    BusinessTravel VARCHAR(100),
    DistanceFromHome INT
);

CREATE TABLE JobDetails (
	JobDetailID INT PRIMARY KEY AUTO_INCREMENT,
    EmployeeNumber INT,
    JobRole VARCHAR(100),
    JobLevel INT,
    NumCompaniesWorked INT,
    TotalWorkingYears VARCHAR(100),
    YearsAtCompany INT,
    YearsSinceLastPromotion INT,
    YearsWithCurrManager INT,
    FOREIGN KEY (EmployeeNumber) REFERENCES Employees(EmployeeNumber)
);

CREATE TABLE Compensation (
    CompensationID INT PRIMARY KEY AUTO_INCREMENT,
    EmployeeNumber INT,
    DailyRate INT,
    HourlyRate INT,
    MonthlyIncome INT,
    MonthlyRate INT,
    StockOptionLevel INT,
    PercentSalaryHike INT,
    OverTime VARCHAR(100),
    TrainingTimesLastYear INT,
    RemoteWork VARCHAR(100),
    FOREIGN KEY (EmployeeNumber) REFERENCES Employees(EmployeeNumber)
);

CREATE TABLE Satisfaction (
    SatisfactionID INT PRIMARY KEY AUTO_INCREMENT,
    EmployeeNumber INT,
    EnvironmentSatisfaction INT,
    JobInvolvement INT,
    JobSatisfaction INT,
    RelationshipSatisfaction INT,
    WorkLifeBalance INT,
    PerformanceRating VARCHAR(100),
    FOREIGN KEY (EmployeeNumber) REFERENCES Employees(EmployeeNumber)
);
