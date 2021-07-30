package ru.yoomoney.tech.dbqueue;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOptions;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * @author Oleg Kandaurov
 * @since 03.08.2017
 */
public class ArchitectureTest {

    private static final String BASE_PACKAGE = "ru.yoomoney.tech.dbqueue";

    private JavaClasses classes;

    @Before
    public void importClasses() {
        classes = new ClassFileImporter(new ImportOptions())
                .importPackages(BASE_PACKAGE);
    }

    @Test
    public void test2() {
        ArchRule rule = classes().that().resideInAnyPackage(
                        fullNames("api"))
                .should().accessClassesThat().resideInAnyPackage(fullNames("api..", "settings.."))
                .orShould().accessClassesThat().resideInAnyPackage("java..")
                .because("api must not depend on implementation details");
        rule.check(classes);
    }

    @Test
    public void test3() {
        ArchRule rule = classes().that().resideInAnyPackage(
                        fullNames("settings"))
                .should().accessClassesThat().resideInAnyPackage(fullNames("settings"))
                .orShould().accessClassesThat().resideInAnyPackage("java..")
                .because("settings must not depend on implementation details");
        rule.check(classes);
    }

    @Test
    public void test4() {
        ArchRule rule = noClasses().that().resideInAnyPackage(
                        fullNames("settings..", "api..", "dao..", "spring.."))
                .should().accessClassesThat().resideInAnyPackage(fullNames("internal.."))
                .because("public classes must not depend on internal details");
        rule.check(classes);
    }


    private static String[] fullNames(String... relativeName) {
        return Arrays.stream(relativeName).map(name -> BASE_PACKAGE + "." + name)
                .collect(Collectors.toList()).toArray(new String[relativeName.length]);
    }

}
