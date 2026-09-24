// Tooling test: verifies repository configuration, not a requirement.
//
// The package version is stated in three places that must agree: the
// `version` of `pubspec.yaml`, `LibVersion.version` (the version the
// library records in the log at every open by a different build), and the
// latest release heading of `CHANGELOG.md`. The data format the latest
// CHANGELOG entry states ("data format X.Y") is `LibVersion.dataFormat`.
@TestOn('vm')
library;

import 'dart:io';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

/// The `version:` of a pubspec's text, or null.
String? pubspecVersion(String pubspec) => RegExp(
  r'^version:\s*(\S+)\s*$',
  multiLine: true,
).firstMatch(pubspec)?.group(1);

/// The first `## <version>` heading of a changelog's text, or null.
String? latestChangelogVersion(String changelog) =>
    RegExp(r'^## (\S+)\s*$', multiLine: true).firstMatch(changelog)?.group(1);

/// The data format ("data format X.Y") the latest entry of a changelog's
/// text states, or null when it states none.
String? latestChangelogDataFormat(String changelog) {
  final headings = RegExp(
    r'^## \S+\s*$',
    multiLine: true,
  ).allMatches(changelog).toList();
  if (headings.isEmpty) return null;
  final end = headings.length > 1 ? headings[1].start : changelog.length;
  final entry = changelog.substring(headings.first.end, end);
  return RegExp(r'data format (\d+\.\d+)').firstMatch(entry)?.group(1);
}

/// Every disagreement between the stated versions; empty when they agree.
List<String> versionProblems({
  required String pubspec,
  required String changelog,
  required String libVersion,
  required String dataFormat,
}) => <String>[
  if (pubspecVersion(pubspec) != libVersion)
    'pubspec version ${pubspecVersion(pubspec)} is not $libVersion',
  if (latestChangelogVersion(changelog) != libVersion)
    'CHANGELOG entry ${latestChangelogVersion(changelog)} is not $libVersion',
  if (latestChangelogDataFormat(changelog) != dataFormat)
    'CHANGELOG data format ${latestChangelogDataFormat(changelog)} is not $dataFormat',
];

void main() {
  test('pubspec, LibVersion and the CHANGELOG agree', () {
    expect(
      versionProblems(
        pubspec: File('pubspec.yaml').readAsStringSync(),
        changelog: File('CHANGELOG.md').readAsStringSync(),
        libVersion: LibVersion.version,
        dataFormat: '${LibVersion.dataFormat}',
      ),
      isEmpty,
    );
  });

  group('the check refuses', () {
    const changelog =
        '# Changelog\n\n## 0.5.0\n\nData: data format 2.0.\n\n'
        '## 0.4.0\n\ndata format 1.0\n';
    const pubspec = 'name: x\nversion: 0.5.0\n';

    test('agreeing statements pass', () {
      expect(
        versionProblems(
          pubspec: pubspec,
          changelog: changelog,
          libVersion: '0.5.0',
          dataFormat: '2.0',
        ),
        isEmpty,
      );
    });

    test('a pubspec of another version', () {
      expect(
        versionProblems(
          pubspec: 'name: x\nversion: 0.4.9\n',
          changelog: changelog,
          libVersion: '0.5.0',
          dataFormat: '2.0',
        ),
        <Matcher>[contains('pubspec version 0.4.9')],
      );
    });

    test('a changelog whose latest entry is another version', () {
      expect(
        versionProblems(
          pubspec: pubspec,
          changelog: '# Changelog\n\n## 0.6.0\n\n$changelog',
          libVersion: '0.5.0',
          dataFormat: '2.0',
        ),
        contains(contains('CHANGELOG entry 0.6.0')),
      );
    });

    test('a data format that differs, or only an older entry states', () {
      expect(
        versionProblems(
          pubspec: pubspec,
          changelog: changelog,
          libVersion: '0.5.0',
          dataFormat: '2.1',
        ),
        <Matcher>[contains('data format 2.0 is not 2.1')],
      );
      expect(
        latestChangelogDataFormat(
          '## 0.5.0\n\nNone.\n\n## 0.4.0\n\ndata format 1.0\n',
        ),
        isNull,
      );
    });

    test('a changelog without a release heading', () {
      expect(latestChangelogVersion('# Changelog\n\nNothing yet.\n'), isNull);
    });
  });
}
