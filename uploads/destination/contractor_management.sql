-- Department table data
INSERT INTO contractor_management.department(department_id, name, location) 
VALUES 
(1, 'HR', 'New York'), 
(2, 'Engineering', 'San Francisco'), 
(3, 'Sales', 'France'), 
(4, 'Finance', 'Boston'),
(5, 'Marketing', 'Seattle'), 
(6, 'IT', 'Austin'), 
(7, 'Admin', 'Denver'), 
(8, 'Support', 'Miami'), 
(9, 'Legal', 'Atlanta'),
(10, 'Operations', 'Dallas');

-- Contractor table data
INSERT INTO contractor_management.contractor(contractor_id, name, age, address, department_id) 
VALUES 
(1, 'Amit', 30, 'NY Address', 1), 
(2, 'Bob', 32, 'SF Address', 2), 
(3, 'Charlie', 59, 'Chicago Address', 3), 
(4, 'David', 35, 'Boston Address', 4), 
(5, 'Eva', 28, 'Seattle Address', 5), 
(6, 'Frank', 40, 'Austin Address', 6),
(7, 'Grace', 33, 'Denver Address', 7), 
(8, 'Hank', 31, 'Miami Address', 8), 
(9, 'Ivy', 27, 'Atlanta Address', 9), 
(10, 'Jack', 36, 'Dallas Address', 10);

-- Attendance table data
INSERT INTO contractor_management.attendance(attendance_id, contractor_id, attendance_date, status) 
VALUES 
(1, 1, '2024-04-01', 'Present'), 
(2, 2, '2024-04-01', 'Absent'), 
(3, 3, '2024-04-01', 'Present'), 
(4, 4, '2024-04-01', 'Absent'), 
(5, 5, '2024-04-01', 'Late'), 
(6, 6, '2024-04-01', 'Present'), 
(7, 7, '2024-04-01', 'Absent'), 
(8, 8, '2024-04-01', 'Present'), 
(9, 9, '2024-04-01', 'Early'), 
(10, 10, '2024-04-01', 'Absent');

-- Leave requests table data
INSERT INTO contractor_management.leaverequests(leave_id, contractor_id, leave_type, start_date, end_date, status) 
VALUES 
(1, 1, 'Sick', '2024-04-05', '2024-04-07', 'Approved'), 
(2, 2, 'Casual', '2024-04-10', '2024-04-11', 'Submitted'), 
(3, 3, 'Vacation', '2024-04-15', '2024-04-20', 'Rejected'), 
(4, 4, 'Sick', '2024-04-08', '2024-04-09', 'Approved'), 
(5, 5, 'Casual', '2024-04-03', '2024-04-04', 'Submitted'), 
(6, 6, 'Vacation', '2024-04-12', '2024-04-14', 'Approved'), 
(7, 7, 'Sick', '2024-04-01', '2024-04-02', 'Rejected'), 
(8, 8, 'Casual', '2024-04-18', '2024-04-19', 'Approved'), 
(9, 9, 'Vacation', '2024-04-06', '2024-04-08', 'Pending'), 
(10, 10, 'Sick', '2024-04-21', '2024-04-22', 'Submitted');

-- Payroll table data
INSERT INTO contractor_management.payroll(payroll_id, contractor_id, payment_date, amount) 
VALUES 
(1, 1, '2024-03-31', 9000.00), 
(2, 2, '2024-03-31', 6000.00), 
(3, 3, '2024-03-31', 4900.00), 
(4, 4, '2024-03-31', 7000.00), 
(5, 5, '2024-03-31', 5220.00), 
(6, 6, '2024-03-31', 9100.00), 
(7, 7, '2024-03-31', 2900.00), 
(8, 8, '2024-03-31', 6800.00), 
(9, 9, '2024-03-31', 4710.00), 
(10, 10, '2024-03-31', 7390.00);

-- Performance reviews table data
INSERT INTO contractor_management.performancereviews(review_id, contractor_id, review_date, score, comments) 
VALUES 
(1, 1, '2024-03-15', 85, 'Good work'), 
(2, 2, '2024-03-15', 90, 'Superb'), 
(3, 3, '2024-03-15', 75, 'Satisfy'), 
(4, 4, '2024-03-15', 80, 'Consistent performer'), 
(5, 5, '2024-03-15', 88, 'Great progress'), 
(6, 6, '2024-03-15', 70, 'Needs improvement'), 
(7, 7, '2024-03-15', 92, 'Outstanding'), 
(8, 8, '2024-03-15', 78, 'Above average'), 
(9, 9, '2024-03-15', 83, 'Reliable'), 
(10, 10, '2024-03-15', 76, 'Can''t do better');

-- Training records table data
INSERT INTO contractor_management.trainingrecords(training_id, contractor_id, course_name, completed_date) 
VALUES 
(1, 1, 'Time Management', '2024-02-01'), 
(2, 2, 'Python', '2024-02-02'), 
(3, 3, 'Agile Basics', '2024-02-03'), 
(4, 4, 'Java Mastery', '2024-02-04'), 
(5, 5, 'Advanced Excel', '2024-02-05'), 
(6, 6, 'Presentation Skills', '2024-02-06'), 
(7, 7, 'docker', '2024-08-01'), 
(8, 8, 'Security Essentials', '2024-02-08'), 
(9, 9, 'Data Analysis', '2024-02-09'), 
(10, 10, 'Conflict Management', '2024-02-10');

-- Assets assigned table data
INSERT INTO contractor_management.assetsassigned(asset_id, contractor_id, asset_name, assigned_date) 
VALUES 
(1, 1, 'Mac', '2024-01-01'), 
(2, 2, 'Mobile', '2024-01-02'), 
(3, 3, 'Tablet', '2024-01-03'), 
(4, 4, 'Monitor', '2024-01-04'), 
(5, 5, 'Headset', '2024-01-05'), 
(6, 6, 'Keyboard', '2024-01-06'), 
(7, 7, 'Mouse', '2024-01-07'), 
(8, 8, 'Docking Station', '2022-06-08'), 
(9, 9, 'Webcam', '2024-01-09'), 
(10, 10, 'Printer', '1999-01-10');

-- Emergency contacts table data
INSERT INTO contractor_management.emergencycontacts(contact_id, contractor_id, contact_name, relation, phone_number) 
VALUES 
(1, 1, 'Mary', 'Husband', '1234567678'), 
(2, 2, 'John', 'Father', '1234567891'), 
(3, 3, 'Kate', 'Sister', '1234567892'), 
(4, 4, 'Mike', 'Brother', '1234567893'), 
(5, 5, 'Linda', 'Spouse', '1234567894'), 
(6, 6, 'Tom', 'Uncle', '1234567895'), 
(7, 7, 'Sara', 'Aunt', '1234567896'), 
(8, 8, 'Leo', 'Friend', '9867464647'), 
(9, 9, 'Tina', 'Cousin', '1234567898'), 
(10, 10, 'Nina', 'Neighbor', '1234567899');

-- Transaction history table data
INSERT INTO contractor_management.transactionhistory_contractor(transaction_id, contractor_id, action_type, description, timestamp) 
VALUES 
(2, 2, 'Payroll', 'Processed salary', '2024-04-01'), 
(5, 5, 'Payroll', 'Salary credited', '2024-04-01'), 
(8, 8, 'Payroll', 'Monthly salary paid', '2024-04-01');