import unittest

from pydicom.dataset import Dataset

from heimdallr.prepare.worker import build_reference_dicom_context


class TestPrepareReferenceDicomContext(unittest.TestCase):
    def test_reference_context_preserves_grouping_tags_and_person_names(self):
        ds = Dataset()
        ds.StudyInstanceUID = "1.2.3"
        ds.StudyID = "STUDY-42"
        ds.StudyDate = "20260405"
        ds.StudyTime = "101112.123"
        ds.StudyDescription = "CT ABDOMEN"
        ds.AccessionNumber = "ACC123"
        ds.SeriesDate = "20260406"
        ds.SeriesTime = "121314.567"
        ds.PatientName = "SILVA^JOAO"
        ds.PatientID = "P001"
        ds.IssuerOfPatientID = "HOSPITAL_A"
        ds.PatientSex = "M"
        ds.PatientBirthDate = "19800115"
        ds.PatientBirthTime = "074500"
        ds.InstitutionName = "General Hospital"
        ds.InstitutionAddress = "Main St"
        ds.StationName = "CT01"
        ds.ReferringPhysicianName = "DOE^JANE"
        ds.PerformingPhysicianName = "ROE^JOHN"
        ds.OperatorsName = "TECH^ONE"
        ds.FrameOfReferenceUID = "1.2.3.4.5"
        ds.BodyPartExamined = "ABDOMEN"

        context = build_reference_dicom_context(ds)

        self.assertEqual(context["StudyInstanceUID"], "1.2.3")
        self.assertEqual(context["StudyID"], "STUDY-42")
        self.assertEqual(context["StudyDescription"], "CT ABDOMEN")
        self.assertEqual(context["SeriesDate"], "20260406")
        self.assertEqual(context["SeriesTime"], "121314.567")
        self.assertEqual(context["PatientName"], "SILVA^JOAO")
        self.assertEqual(context["IssuerOfPatientID"], "HOSPITAL_A")
        self.assertEqual(context["PatientBirthDate"], "19800115")
        self.assertEqual(context["PatientBirthTime"], "074500")
        self.assertEqual(context["InstitutionName"], "General Hospital")
        self.assertEqual(context["InstitutionAddress"], "Main St")
        self.assertEqual(context["StationName"], "CT01")
        self.assertEqual(context["ReferringPhysicianName"], "DOE^JANE")
        self.assertEqual(context["PerformingPhysicianName"], "ROE^JOHN")
        self.assertEqual(context["OperatorsName"], "TECH^ONE")
        self.assertEqual(context["FrameOfReferenceUID"], "1.2.3.4.5")
        self.assertEqual(context["BodyPartExamined"], "ABDOMEN")


if __name__ == "__main__":
    unittest.main()
