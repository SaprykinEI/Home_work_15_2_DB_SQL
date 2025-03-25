
def check_doctors():
    QUERY = """SELECT d.Id, d.Name 
FROM [Hospital].[dbo].[Departments] d
WHERE EXISTS (
    SELECT 1 
    FROM [Hospital].[dbo].[DoctorsExaminations] de
    JOIN [Hospital].[dbo].[Wards] w ON de.WardId = w.Id
    JOIN [Hospital].[dbo].[Doctors] doc ON de.DoctorId = doc.Id
    WHERE doc.Id = de.DoctorId
);
            """
    return QUERY

def queries_exist_1():
    QUERY = fr"""SELECT Name
FROM Sponsors AS s
WHERE EXISTS (
    SELECT 1
    FROM donations AS d
    WHERE s.id = d.SponsorsId
);
            """
    return QUERY

def queries_exist_2():
    QUERY = fr"""SELECT d.Name
                FROM [Hospital].[dbo].[Doctors] d
                WHERE EXISTS (
                    SELECT 1
                    FROM [Hospital].[dbo].[DoctorsExaminations] de
                    WHERE d.Id = de.DoctorId
                    AND de.StartTime < '2025-01-01 12:00:00'
                );
                """
    return QUERY

def queries_any():
    QUERY = fr"""SELECT Name 
FROM [Hospital].[dbo].[Departments] 
WHERE Id = ANY (SELECT DepartmentId FROM [Hospital].[dbo].[Wards]);

"""
    return QUERY

def queries_some():
    QUERY = fr"""SELECT Name, Salary 
FROM [Hospital].[dbo].[Doctors] 
WHERE Salary > SOME (SELECT Salary FROM [Hospital].[dbo].[Doctors] WHERE Salary < 100000);
"""
    return QUERY

def queries_all():
    QUERY = fr"""SELECT Name
FROM [Hospital].[dbo].[Wards]
WHERE Places > ALL (
    SELECT Places
    FROM [Hospital].[dbo].[Wards]
    WHERE DepartmentId = 1
);
"""
    return QUERY

def queries_all_any():
    QUERY = fr"""SELECT Surname, Name, Salary
FROM [Hospital].[dbo].[Doctors]
WHERE Salary > ALL (
    SELECT Salary
    FROM [Hospital].[dbo].[Doctors]
    WHERE Salary < 50000
)
OR Salary < ANY (
    SELECT Salary
    FROM [Hospital].[dbo].[Doctors]
    WHERE Salary > 100000
);
"""
    return QUERY

def queries_union():
    QUERY = fr"""SELECT Id, Name, Building
FROM [Hospital].[dbo].[Departments]
WHERE Name LIKE 'Н%'

UNION

SELECT Id, Name, Building
FROM [Hospital].[dbo].[Departments]
WHERE Name LIKE 'О%';
"""
    return QUERY

def queries_union_all():
    QUERY = fr"""SELECT Surname, Name, Salary
FROM [Hospital].[dbo].[Doctors]
WHERE Salary > 50000

UNION ALL

SELECT Surname, Name, Salary
FROM [Hospital].[dbo].[Doctors]
WHERE Salary < 20000;
"""
    return QUERY

def queries_inner_join():
    QUERY = fr"""SELECT Doctors.Name, Doctors.Surname, Wards.Name
FROM [Hospital].[dbo].[Doctors] 
INNER JOIN [Hospital].[dbo].[DoctorsExaminations] ON Doctors.Id = DoctorsExaminations.DoctorId
INNER JOIN [Hospital].[dbo].[Wards] ON DoctorsExaminations.WardId = Wards.Id;

"""
    return QUERY

def queries_left_join():
    QUERY = fr"""SELECT
    Doctors.Name AS DoctorName,
    Doctors.Surname,
    Examinations.Name AS ExaminationName,
    Wards.Name AS WardName
FROM
    [Hospital].[dbo].[Doctors]
LEFT JOIN
    [Hospital].[dbo].[DoctorsExaminations] ON Doctors.Id = DoctorsExaminations.DoctorId
LEFT JOIN
    [Hospital].[dbo].[Examinations] ON DoctorsExaminations.ExaminationId = Examinations.Id
LEFT JOIN
    [Hospital].[dbo].[Wards] ON DoctorsExaminations.WardId = Wards.Id;

"""
    return QUERY


def queries_right_join():
    QUERY = fr"""SELECT
    Sponsors.Name AS SponsorName,
    Donations.Amount,
    Donations.Date,
    Departments.Name AS DepartmentName
FROM
    [Hospital].[dbo].[Sponsors]
RIGHT JOIN
    [Hospital].[dbo].[Donations] ON Sponsors.Id = Donations.SponsorsId
RIGHT JOIN
    [Hospital].[dbo].[Departments] ON Donations.DepartmentId = Departments.Id;

"""
    return QUERY

def queries_left_right_join():
    QUERY = fr"""SELECT
    Doctors.Name AS DoctorName,
    Doctors.Surname,
    Examinations.Name AS ExaminationName,
    Wards.Name AS WardName,
    Departments.Name AS DepartmentName
FROM
    [Hospital].[dbo].[Doctors]
LEFT JOIN
    [Hospital].[dbo].[DoctorsExaminations] ON Doctors.Id = DoctorsExaminations.DoctorId
LEFT JOIN
    [Hospital].[dbo].[Examinations] ON DoctorsExaminations.ExaminationId = Examinations.Id
LEFT JOIN
    [Hospital].[dbo].[Wards] ON DoctorsExaminations.WardId = Wards.Id
RIGHT JOIN
    [Hospital].[dbo].[Departments] ON Wards.DepartmentId = Departments.Id;

"""
    return QUERY


def queries_full_join():
    QUERY = fr"""SELECT
    Sponsors.Name AS SponsorName,
    Donations.Amount,
    Donations.Date,
    Departments.Name AS DepartmentName,
    Wards.Name AS WardName
FROM
    [Hospital].[dbo].[Sponsors]
FULL JOIN
    [Hospital].[dbo].[Donations] ON Sponsors.Id = Donations.SponsorsId
FULL JOIN
    [Hospital].[dbo].[Departments] ON Donations.DepartmentId = Departments.Id
FULL JOIN
    [Hospital].[dbo].[Wards] ON Departments.Id = Wards.DepartmentId;

"""
    return QUERY