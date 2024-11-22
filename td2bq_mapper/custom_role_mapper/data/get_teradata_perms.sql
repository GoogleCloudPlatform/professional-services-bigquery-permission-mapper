-- Copyright 2021 Google LLC

-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at

--     https://www.apache.org/licenses/LICENSE-2.0

-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

SELECT
  Username(Varchar(128)) AS UserName,
  Accesstype(Varchar(128)) AS AccessType,
  Rolename(Varchar(128)) AS RoleName,
  Databasename(Varchar(128)) AS DatabaseName,
  Tablename(Varchar(128)) AS TableName,
  Columnname(Varchar(128)) AS ColumnName,
  accessright AS AccessRight,
  CASE
    WHEN accessRight = 'AE' THEN 'ALTER EXTERNALPROCEDURE'
    WHEN AccessRight = 'AF' THEN 'ALTER FUNCTION'
    WHEN AccessRight = 'AP' THEN 'ALTER PROCEDURE'
    WHEN AccessRight = 'AS' THEN 'ABORT SESSION'
    WHEN AccessRight = 'CA' THEN 'CREATE AUTHORIZATION'
    WHEN AccessRight = 'CD' THEN 'CREATE DATABASE'
    WHEN AccessRight = 'CE' THEN 'CREATE EXTERNAL PROCEDURE'
    WHEN AccessRight = 'CF' THEN 'CREATE FUNCTION'
    WHEN AccessRight = 'CG' THEN 'CREATE TRIGGER'
    WHEN AccessRight = 'CM' THEN 'CREATE MACRO'
    WHEN AccessRight = 'CO' THEN 'CREATE PROFILE'
    WHEN AccessRight = 'CP' THEN 'CHECKPOINT'
    WHEN AccessRight = 'CR' THEN 'CREATE ROLE'
    WHEN AccessRight = 'CT' THEN 'CREATE TABLE'
    WHEN AccessRight = 'CU' THEN 'CREATE USER'
    WHEN AccessRight = 'CV' THEN 'CREATE VIEW'
    WHEN AccessRight = 'D' THEN 'DELETE'
    WHEN AccessRight = 'CZ' THEN 'CREATE ZONE'
    WHEN AccessRight = 'DA' THEN 'DROP AUTHORIZATION'
    WHEN AccessRight = 'DD' THEN 'DROP DATABASE'
    WHEN AccessRight = 'DF' THEN 'DROP FUNCTION'
    WHEN AccessRight = 'DG' THEN 'DROP TRIGGER'
    WHEN AccessRight = 'DM' THEN 'DROP MACRO'
    WHEN AccessRight = 'DO' THEN 'DROP PROFILE'
    WHEN AccessRight = 'DP' THEN 'DUMP'
    WHEN AccessRight = 'DR' THEN 'DROP ROLE'
    WHEN AccessRight = 'DT' THEN 'DROP TABLE'
    WHEN AccessRight = 'DU' THEN 'DROP USER'
    WHEN AccessRight = 'DV' THEN 'DROP VIEW'
    WHEN AccessRight = 'E' THEN 'EXECUTE'
    WHEN AccessRight = 'DZ' THEN 'DROP ZONE'
    WHEN AccessRight = 'EF' THEN 'EXECUTE FUNCTION'
    WHEN AccessRight = 'GC' THEN 'CREATE GLOP'
    WHEN AccessRight = 'GD' THEN 'DROP GLOP'
    WHEN AccessRight = 'GM' THEN 'GLOP MEMBER'
    WHEN AccessRight = 'I' THEN 'INSERT'
    WHEN AccessRight = 'IX' THEN 'INDEX'
    WHEN AccessRight = 'MR' THEN 'MONITOR RESOURCE'
    WHEN AccessRight = 'MS' THEN 'MONITOR SESSION'
    WHEN AccessRight = 'NT' THEN 'NONTEMPORAL'
    WHEN AccessRight = 'OD' THEN 'OVERRIDE DELETE POLICY'
    WHEN AccessRight = 'OI' THEN 'OVERRIDE INSERT POLICY'
    WHEN AccessRight = 'OP' THEN 'CREATE OWNER PROCEDURE'
    WHEN AccessRight = 'OS' THEN 'OVERRIDE SELECT POLICY'
    WHEN AccessRight = 'OU' THEN 'OVERRIDE UPDATE POLICY'
    WHEN AccessRight = 'PC' THEN 'CREATE PROCEDURE'
    WHEN AccessRight = 'PD' THEN 'DROP PROCEDURE'
    WHEN AccessRight = 'PE' THEN 'EXECUTE PROCEDURE'
    WHEN AccessRight = 'R' THEN 'RETRIEVE/SELECT'
    WHEN AccessRight = 'RF' THEN 'REFERENCES'
    WHEN AccessRight = 'RS' THEN 'RESTORE'
    WHEN AccessRight = 'SA' THEN 'SECURITY CONSTRAINT ASSIGNMENT'
    WHEN AccessRight = 'SD' THEN 'SECURITY CONSTRAINT DEFINITION'
    WHEN AccessRight = 'ST' THEN 'STATISTICS'
    WHEN AccessRight = 'SS' THEN 'SET SESSION RATE'
    WHEN AccessRight = 'SR' THEN 'SET RESOURCE RATE'
    WHEN AccessRight = 'TH' THEN 'CTCONTROL'
    WHEN AccessRight = 'U' THEN 'UPDATE'
    WHEN AccessRight = 'UU' THEN 'UDT Usage'
    WHEN AccessRight = 'UT' THEN 'UDT Type'
    WHEN AccessRight = 'UM' THEN 'UDT Method'
    WHEN AccessRight = 'ZO' THEN 'ZONE OVERRIDE'
    ELSE ''
    END(varchar(26)) AS AccessRightDesc,
  grantauthority AS GrantAuthority,
  grantorname(varchar(128)) AS GrantorName,
  allnessflag AS AllnessFlag,
  creatorname(varchar(128)) AS CreatorName,
  createtimestamp AS CreateTimeStamp
FROM
  (
    SELECT
      username,
      'User' (varchar(128)) AS accesstype,
      '' (varchar(128)) AS rolename,
      databasename,
      tablename,
      columnname,
      accessright,
      grantauthority,
      grantorname,
      allnessflag,
      creatorname,
      createtimestamp
    FROM dbc.allrights
    UNION ALL
    SELECT
      grantee AS username,
      'Member' AS accesstype,
      r.rolename,
      databasename,
      tablename,
      columnname,
      accessright,
      NULL(char(1)) AS grantauthority,
      grantorname,
      NULL(char(1)) AS allnessflag,
      NULL(char(1)) AS creatorname,
      createtimestamp
    FROM dbc.allrolerights r
    JOIN dbc.rolemembers m
      ON m.rolename = r.rolename
    UNION ALL
    SELECT
      USER AS username,
      m.grantee AS accesstype,
      r.rolename,
      databasename,
      tablename,
      columnname,
      accessright,
      NULL(char(1)) AS grantauthority,
      grantorname,
      NULL(char(1)) AS allnessflag,
      NULL(char(1)) AS creatorname,
      createtimestamp
    FROM dbc.allrolerights r
    JOIN dbc.rolemembers m
      ON m.rolename = r.rolename
  ) allrights
WHERE
  username NOT IN (
    'TDPUSER',
    'Crashdumps',
    'tdwm',
    'DBC',
    'LockLogShredder',
    'TDMaps',
    'Sys_Calendar',
    'SysAdmin',
    'SystemFe',
    'External_AP')
  AND username IN ()
  AND rolename IN ()