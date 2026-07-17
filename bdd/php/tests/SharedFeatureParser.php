<?php
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

declare(strict_types=1);

final class SharedFeatureParser
{
    public static function load(string $featureFile): array
    {
        if (!is_file($featureFile)) {
            throw new RuntimeException("feature file not found at {$featureFile}");
        }

        $lines = file($featureFile, FILE_IGNORE_NEW_LINES);
        if ($lines === false) {
            throw new RuntimeException("failed to read feature file at {$featureFile}");
        }

        $backgroundSteps = [];
        $scenarios = [];
        $currentScenario = null;
        $currentSteps = [];
        $section = null;
        foreach ($lines as $line) {
            $line = trim($line);
            if ($line === '' || str_starts_with($line, '#') || str_starts_with($line, '@')) {
                continue;
            }

            if ($line === 'Background:') {
                $section = 'background';
                continue;
            }

            if (str_starts_with($line, 'Scenario:')) {
                if ($currentScenario !== null) {
                    $scenarios[$currentScenario] = $currentSteps;
                }
                $currentScenario = trim(substr($line, strlen('Scenario:')));
                $currentSteps = [];
                $section = 'scenario';
                continue;
            }

            if (preg_match('/^(Scenario Outline|Rule|Examples):|^\|/', $line) === 1) {
                throw new RuntimeException("Unsupported BDD structure: {$line}");
            }

            if (preg_match('/^(Given|When|Then|And|But|\*) (.+)$/', $line, $matches) !== 1) {
                continue;
            }
            if ($section === null) {
                throw new RuntimeException("BDD step appears before Background or Scenario: {$line}");
            }
            if ($section === 'background') {
                $backgroundSteps[] = $matches[2];
            } elseif ($currentScenario !== null) {
                $currentSteps[] = $matches[2];
            }
        }

        if ($currentScenario !== null) {
            $scenarios[$currentScenario] = $currentSteps;
        }
        if ($backgroundSteps === [] || $scenarios === []) {
            throw new RuntimeException("feature must contain a background and at least one scenario: {$featureFile}");
        }

        $cases = [];
        foreach ($scenarios as $scenarioName => $scenarioSteps) {
            if ($scenarioSteps === []) {
                throw new RuntimeException("scenario has no steps: {$scenarioName}");
            }
            $cases[$scenarioName] = [$scenarioName, [...$backgroundSteps, ...$scenarioSteps]];
        }

        return $cases;
    }
}
