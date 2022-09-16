<?php

namespace App\DTO\input;

use Illuminate\Http\Request;

class PrebidSourceDTO
{
    public ?string $dspCompanyName;
    public ?string $source;
    public ?array $prebid;
    public ?string $partner;

    /**
     * @param string|null $dspCompanyName
     * @param string|null $source
     * @param array|null $prebid
     * @param string|null $partner
     */
    public function __construct(?string $dspCompanyName, ?string $source, ?array $prebid, ?string $partner)
    {
        $this->dspCompanyName = $dspCompanyName;
        $this->source = $source;
        $this->prebid = $prebid;
        $this->partner = $partner;
    }

    public static function fromRequest(Request $request): self {
        return new self(
            $request->get('dspCompanyName'),
            $request->get('source'),
            $request->get('prebid'),
            $request->get('partner'),
        );
    }

}
