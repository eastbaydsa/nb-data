# EBDSA NationBuilder management scripts

- [Committee Membership](#committee-membership)

---

## Dependencies

Ruby scripts require Ruby 2.2 or greater.

## Committee Membership

`(committee_membership.rb)`

This script pulls the list of all members, examines their
assigned tags, and checks these tags against the criteria
for committee membership (varies per committee). It creates
a list of members, and assigns a copy of it to each committee's
co-chairs and recording secretary.

#### Options

All options to this script are passed as environment variables.

Required:

- `TOKEN`: a NationBuilder API token
- `NATION`: a NationBuilder Nation ID

Optional:

- `MEMBER_TAG`: a tag to identify members with; defaults
  to `national_member`

#### Example usage

```sh
$ NATION=eastbaydsa TOKEN=XXXX ruby committee_membership.rb
Fetching members...
Building membership roster...
Fetching lists...
Removing existing list: 123 (mbrs_housing_2)
Removing existing list: 124 (mbrs_housing_1)
Assigning committee housing list 1 to housing-chair-1@gmail.com
Assigning committee housing list 2 to housing-chair-2@gmail.com
```
