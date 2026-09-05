package sessionstore

import (
	"reflect"
	"sort"
	"testing"
	"time"
)

// Every durable record below has its stored member names pinned by a private
// DTO and carried across by a hand-written, member-by-member conversion rather
// than by a whole-struct conversion. That choice keeps every single-member drop
// expressible as a mutation, but it gives up the one thing a conversion buys:
// the compiler does NOT check that the conversions are total. A member added to
// an exported record AND its DTO while both conversions are forgotten compiles
// and leaves the rest of the suite green while the member is silently dropped on
// encode and on decode. These two tests stand in for the compiler.
//
// TestWireDTOsMirrorExportedRecords fails when a pair stops having the same
// members, which is what a member added to only one of the two looks like.
func TestWireDTOsMirrorExportedRecords(t *testing.T) {
	pairs := []struct {
		exported, wire reflect.Type
	}{
		{reflect.TypeOf(DispositionInboxRecord{}), reflect.TypeOf(dispositionInboxRecordWire{})},
		{reflect.TypeOf(DispositionCommandDescriptor{}), reflect.TypeOf(dispositionDescriptorWire{})},
		{reflect.TypeOf(PublicCreateReservation{}), reflect.TypeOf(publicCreateReservationWire{})},
		{reflect.TypeOf(PublicCreateIdentity{}), reflect.TypeOf(publicCreateIdentityWire{})},
		{reflect.TypeOf(HostTargetKey{}), reflect.TypeOf(publicCreateTargetWire{})},
		{reflect.TypeOf(DesiredWorkload{}), reflect.TypeOf(desiredWorkloadWire{})},
	}
	if len(pairs) == 0 {
		t.Fatal("vacuous: no pinned exported/DTO pairs were examined")
	}
	for _, pair := range pairs {
		t.Run(pair.exported.Name(), func(t *testing.T) {
			exported, wire := memberNames(t, pair.exported), memberNames(t, pair.wire)
			if len(exported) == 0 {
				t.Fatalf("vacuous: %s has no members", pair.exported)
			}
			if !reflect.DeepEqual(exported, wire) {
				t.Fatalf("%s has members %v but %s has %v: a member added to one side must be added to the other AND mapped by hand in both conversions, because the compiler will not say so",
					pair.exported, exported, pair.wire, wire)
			}
		})
	}
}

// TestWireConversionsCarryEveryMember fills every member of a record, including
// every member of every nested record, with a distinguishable non-zero value and
// requires the pair of conversions to return it unchanged. A member present on
// both structs but missing from either conversion is dropped here and fails.
// The two top-level records reach the other four pairs through their nested
// members, so both directions of all six conversions are exercised.
func TestWireConversionsCarryEveryMember(t *testing.T) {
	t.Run("DispositionInboxRecord", func(t *testing.T) {
		var record DispositionInboxRecord
		fillProbeMembers(t, reflect.ValueOf(&record).Elem(), "DispositionInboxRecord")
		if got := dispositionInboxToWire(record).record(); !reflect.DeepEqual(got, record) {
			t.Fatalf("conversion dropped or altered a member\n got: %+v\nwant: %+v", got, record)
		}
	})
	t.Run("PublicCreateReservation", func(t *testing.T) {
		var reservation PublicCreateReservation
		fillProbeMembers(t, reflect.ValueOf(&reservation).Elem(), "PublicCreateReservation")
		if got := publicCreateToWire(reservation).reservation(); !reflect.DeepEqual(got, reservation) {
			t.Fatalf("conversion dropped or altered a member\n got: %+v\nwant: %+v", got, reservation)
		}
	})
}

func memberNames(t *testing.T, typ reflect.Type) []string {
	t.Helper()
	if typ.Kind() != reflect.Struct {
		t.Fatalf("%s is not a struct", typ)
	}
	names := make([]string, 0, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		names = append(names, typ.Field(i).Name)
	}
	sort.Strings(names)
	return names
}

var probeMemberTime = time.Date(2026, 9, 5, 12, 30, 0, 0, time.UTC)

// fillProbeMembers sets value, and everything reachable from it, to a non-zero
// value. It refuses rather than skips anything it cannot fill, so a member of a
// kind it does not handle is a test failure asking for it to be extended, never
// a silently unprobed member.
func fillProbeMembers(t *testing.T, value reflect.Value, path string) {
	t.Helper()
	switch {
	case value.Type() == reflect.TypeOf(time.Time{}):
		value.Set(reflect.ValueOf(probeMemberTime))
	case value.Kind() == reflect.Bool:
		value.SetBool(true)
	case value.Kind() == reflect.String:
		value.SetString("probe:" + path)
	case value.CanUint():
		value.SetUint(7)
	case value.CanInt():
		value.SetInt(7)
	case value.Kind() == reflect.Slice:
		filled := reflect.MakeSlice(value.Type(), 1, 1)
		fillProbeMembers(t, filled.Index(0), path+"[0]")
		value.Set(filled)
	case value.Kind() == reflect.Pointer:
		pointer := reflect.New(value.Type().Elem())
		fillProbeMembers(t, pointer.Elem(), path+"->")
		value.Set(pointer)
	case value.Kind() == reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			member := value.Type().Field(i)
			if !member.IsExported() {
				t.Fatalf("%s.%s is unexported and cannot be carried by a conversion", path, member.Name)
			}
			fillProbeMembers(t, value.Field(i), path+"."+member.Name)
		}
	default:
		t.Fatalf("cannot fill %s of kind %s: extend fillProbeMembers", path, value.Kind())
	}
	if value.IsZero() {
		t.Fatalf("vacuous: %s was left zero, so dropping it would not be detected", path)
	}
}
