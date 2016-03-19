/*
Navicat MySQL Data Transfer

Source Server         : 71MySQL
Source Server Version : 50624
Source Host           : 10.63.0.71:3306
Source Database       : onlinefem

Target Server Type    : MYSQL
Target Server Version : 50624
File Encoding         : 65001

Date: 2015-05-10 14:32:48
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for node
-- ----------------------------
DROP TABLE IF EXISTS `node`;
CREATE TABLE `node` (
  `time` int(11) NOT NULL,
  `trie` longblob NOT NULL,
  UNIQUE KEY `tindex` (`time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
