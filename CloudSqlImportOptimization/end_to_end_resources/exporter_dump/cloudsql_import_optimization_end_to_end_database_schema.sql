-- MySQL dump 10.16  Distrib 10.1.37-MariaDB, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: cloudsql_import_optimization_end_to_end_database
-- ------------------------------------------------------
-- Server version	10.1.37-MariaDB-1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `bar_table`
--

DROP TABLE IF EXISTS `bar_table`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bar_table` (
  `bar_int` int(10) unsigned NOT NULL,
  `bar_var_char` varchar(4000) NOT NULL,
  `bar_str` blob NOT NULL,
  `bar_char` char(255) DEFAULT NULL,
  `bar_tinyint` tinyint(255) unsigned DEFAULT NULL,
  `bar_double` double DEFAULT NULL,
  UNIQUE KEY `bar_int` (`bar_int`),
  KEY `bar_char` (`bar_char`),
  KEY `bar_tinyint` (`bar_tinyint`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `baz_table`
--

DROP TABLE IF EXISTS `baz_table`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `baz_table` (
  `baz_int` int(10) unsigned NOT NULL,
  `baz_var_char` varchar(4000) NOT NULL,
  `baz_str` blob NOT NULL,
  `baz_char` char(255) DEFAULT NULL,
  `baz_tinyint` tinyint(255) unsigned DEFAULT NULL,
  `baz_double` double DEFAULT NULL,
  UNIQUE KEY `baz_int` (`baz_int`),
  KEY `baz_tinyint` (`baz_tinyint`),
  KEY `baz_var_char` (`baz_var_char`(768))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `foo_table`
--

DROP TABLE IF EXISTS `foo_table`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `foo_table` (
  `foo_int` int(10) unsigned NOT NULL,
  `foo_var_char` varchar(4000) NOT NULL,
  `foo_str` blob NOT NULL,
  `foo_char` char(255) DEFAULT NULL,
  `foo_tinyint` tinyint(255) unsigned DEFAULT NULL,
  `foo_double` double DEFAULT NULL,
  UNIQUE KEY `foo_int` (`foo_int`),
  KEY `foo_char` (`foo_char`),
  KEY `foo_tinyint` (`foo_tinyint`),
  KEY `foo_var_char` (`foo_var_char`(500))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed
